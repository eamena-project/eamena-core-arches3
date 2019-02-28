import os
import json
import uuid
import unicodecsv
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
from arches.app.models.concept import Concept
from arches.app.models.models import ConceptRelations, Concepts

from arches.app.utils.skos import SKOSReader

class Command(BaseCommand):

    option_list = BaseCommand.option_list + (
        make_option('-s', '--source', action='store', default=None,
            help="path to skosfile"),
    )

    def handle(self, *args, **options):

        if options['source'] is None:
            filepath = settings.SKOS_FILE_LOCATION

        else:
            if not os.path.isfile(options['source']):
                print "invalid source."
                exit()
            filepath = options['source']

        print "\nCONCEPTS\n--------"
        print "loading skos from file: " + os.path.basename(filepath)
        schemeid = self.load_skos(filepath)

        print "\nDROPDOWNS\n---------"
        collection_file = filepath.replace(".xml","_collections.json")
        lookups_file = filepath.replace(".xml","_lookups.csv")

        if os.path.isfile(collection_file):
            print "loading dropdowns directly from:", collection_file
            self.load_dropdown_data(collection_file)

        elif os.path.isfile(lookups_file):
            print "creating dropdowns based on lookups:", lookups_file
            lookups = self.test_linking_file(lookups_file)
            self.link_dropdowns(schemeid,lookups)
        
        else:
            print "no dropdown data to load."

    def load_skos(self,skosfile):

        skos = SKOSReader()
        rdf = skos.read_file(skosfile)
        ret = skos.save_concepts_from_skos(rdf)

        schemeid = [i.id for i in ret.nodes if i.nodetype == "ConceptScheme"][0]

        return schemeid

    def load_dropdown_data(self, sourcefile):

        with open(sourcefile, "rb") as openf:
            dropdown_data = json.loads(openf.read())

        print len(dropdown_data)
        for name, collection in dropdown_data.iteritems():
            for member in collection:

                # first try to use and existing concept for "from". this is
                # in the case of dropdowns items that are nested within other
                # dropdown items (i.e. concepts)
                try:
                    fromid = Concepts.objects.get(conceptid=member["from"])
                # if "from" doesn't match any existing concepts, assume that
                # this is a connection to the top concept (whose id can change
                # from one installation to the next). in this case, use the
                # name of the top concept (which will be the same from one
                # installation to the next)
                except Concepts.DoesNotExist:
                    try:
                        fromid = Concepts.objects.get(legacyoid=name.upper()).conceptid
                    except Concepts.DoesNotExist:
                        continue
                newrelation = ConceptRelations(
                    relationid = uuid.uuid4(),
                    conceptidfrom_id=fromid,
                    conceptidto_id=member["to"],
                    relationtype_id="member"
                )
                try:
                    newrelation.save()
                except Exception as e:
                    print e
                    print member["from"], member["to"]

    def test_linking_file(self,lfile):
        
        print "loading and testing the lookups file"
        # these are the relations from the dropdown scheme to actual collections
        dropdown_relations = ConceptRelations.objects.filter(
            conceptidfrom_id="00000000-0000-0000-0000-000000000003"
        )

        # these are the ids of all the collections themselves
        collection_ids = [i.conceptidto_id for i in dropdown_relations]
        
        all_node_names = [Concept().get(id=cid).legacyoid for cid in collection_ids]

        lookups = {}

        with open(lfile, "rb") as openfile:
            reader = unicodecsv.reader(openfile)
            headers = reader.next()
            for row in reader:
                lookups[row[0]] = row[1]

        invalid_nodes_in_lookup = [i for i in lookups.keys() if not i in all_node_names]
        
        if len(invalid_nodes_in_lookup) > 0:
            print len(invalid_nodes_in_lookup), "nodes in your lookup file are"\
                "not valid node names. (Disregard this message if your resource)"\
                "graphs have not been loaded yet.)"
    
        nodes_not_in_lookup = [i for i in all_node_names if not i in lookups]
        if nodes_not_in_lookup:
            for nodename in nodes_not_in_lookup:
                lookups[nodename] = ""
            with open(lfile, "wb") as openfile:
                writer = unicodecsv.writer(openfile)
                writer.writerow(headers)
                for name in sorted(lookups):
                    writer.writerow([name,lookups[name]])

        print len(nodes_not_in_lookup), "new node names added to your lookup"
        return lookups

    def link_dropdowns(self,schemeid,lookups):

        print "linking topconcepts with dropdown lists"

        # make the graph of the full scheme
        full_graph = Concept().get(
                id=schemeid,
                include_subconcepts=True,
                # depth_limit=2,
            )

        # make a dictionary of all the top concepts
        topconcepts = {}
        missing_tc = []
        for tc in full_graph.subconcepts:
            for v in tc.values:
                if v.category == "label" and v.language == "en-US":
                    topconcepts[v.value] = tc
                    if not v.value in lookups.values():
                        missing_tc.append(v.value)

        # these are the relations from the dropdown scheme to actual collections
        dropdown_relations = ConceptRelations.objects.filter(
            conceptidfrom_id="00000000-0000-0000-0000-000000000003"
        )

        # these are the ids of all the collections themselves
        collection_ids = [i.conceptidto_id for i in dropdown_relations]
        
        all_node_names = [Concept().get(id=cid).legacyoid for cid in collection_ids]

        # iterate the collection ids. For each get the actual concept,
        # then its name, and then, if it's been linked with a topconcept,
        # add the subconcepts of that topconcept to the dropdown by saving
        # a new ConceptRelations object for the link.
        missing_n = []
        for cid in collection_ids:

            cconcept = Concept().get(id=cid)
            name = cconcept.legacyoid

            if name in lookups:
                
                try:
                    tc = topconcepts[lookups[name]]
                except Exception as e:
                    print e
                    continue
                for sc in tc.subconcepts:
                    newrelation = ConceptRelations(
                        relationid = uuid.uuid4(),
                        conceptidfrom_id=cid,
                        conceptidto_id=sc.id,
                        relationtype_id="member"
                    )
                    newrelation.save()
            else:
                missing_n.append(name)

        # print a summary of the missing linkages
        missing_n.sort()
        print "these nodes have no dropdown contents associated with them:"
        for i in missing_n:
            print " ",i
        missing_tc.sort()
        print "these top concepts are not associated with any nodes:"
        for i in missing_tc:
            print " ", i

        print "DONE"
