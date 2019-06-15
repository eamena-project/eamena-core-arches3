import re
import os
import imp
import unicodecsv
import uuid
import json
import shutil
import traceback
from collections import namedtuple
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
from arches.app.utils.v1v2migration import NewResource
from arches.management.commands.package_utils.resource_graphs import load_graphs
from arches.app.models.entity import Entity
from arches.app.models.concept import Concept
from arches.app.models.models import ConceptRelations, Entities, EntityTypes, Concepts, Values

logdir = os.path.join(settings.PACKAGE_ROOT,"logs")
logfile = os.path.join(logdir,"v1_conversion_general_log.txt")
missed_labels_file = os.path.join(logdir,"v1_conversion_missing_labels.csv")

class Command(BaseCommand):

    verbose = False

    option_list = BaseCommand.option_list + (
        make_option('-s', '--source', action='store',
            help="source v1 json file"),
        make_option('--slice', action='store', default=None,
            help="slice the list of resources"),
        make_option('--remove-eamena-ids', action='store_true', default=False,
            help="removes the eamena id of input resources so it won't be"\
                "retained on load"),
        make_option('--prepare', action='store_true', default=False,
            help="makes the necessary graph changes to prepare for the"\
                "import of v1 data"),
        make_option('--verbose', action='store_true', default=False,
            help="enables extra print statements")
    )

    def handle(self, *args, **options):

        if options['verbose']:
            self.verbose = True
        if options['prepare']:
            self.load_extra_nodes()
        else:
            self.convert_v1_json(options['source'],slice=options['slice'],
                remove_ids=options['remove_eamena_ids'])

    def add_children_to_concept(self, model_entitytype, new_entitytype):

        e1 = EntityTypes.objects.get(entitytypeid=model_entitytype)
        e2 = EntityTypes.objects.get(entitytypeid=new_entitytype)
        cr_before = ConceptRelations.objects.filter(conceptidfrom=e2.conceptid)
        if self.verbose:
            print "relations to new node before:", cr_before.count()

        cr_all = ConceptRelations.objects.filter(conceptidfrom=e1.conceptid)
        if self.verbose:
            print "relations to add:", cr_all.count()
        for cr in cr_all:
            new_relation = ConceptRelations(
                conceptidfrom=e2.conceptid,
                conceptidto=cr.conceptidto,
                relationtype=cr.relationtype,
                relationid=str(uuid.uuid4())
            )
            new_relation.save()

        cr_new = ConceptRelations.objects.filter(conceptidfrom=e2.conceptid)
        if self.verbose:
            print "relations added to new entitytype:", cr_new.count()

    def load_extra_nodes(self):

        extra_graph_dirs = [os.path.join(settings.PACKAGE_ROOT,"source_data",
            "resource_graphs","additional_graphs")]

        # create a namedtuple to pass in as mock settings to the load_graphs commands
        # only the settings attributes that are needed in load_graphs are in this
        # namedtuple.
        Settings = namedtuple("Settings", "RESOURCE_GRAPH_LOCATIONS "
            "LIMIT_ENTITY_TYPES_TO_LOAD PACKAGE_ROOT ROOT_DIR")
        mocksettings = Settings(extra_graph_dirs, None, settings.PACKAGE_ROOT, settings.ROOT_DIR)

        load_graphs(settings=mocksettings)

        # individually set the concept relations to create the dropdown lists for
        # each of the new nodes.
        self.add_children_to_concept("EFFECT_TYPE.I4","EFFECT_TYPE_1.I4")
        self.add_children_to_concept("EFFECT_TYPE.I4","EFFECT_TYPE_2.I4")
        self.add_children_to_concept("EFFECT_TYPE.I4","EFFECT_TYPE_3.I4")
        self.add_children_to_concept("EFFECT_TYPE.I4","EFFECT_TYPE_4.I4")
        self.add_children_to_concept("EFFECT_TYPE.I4","EFFECT_TYPE_5.I4")
        self.add_children_to_concept("EFFECT_CERTAINTY.I6","EFFECT_CERTAINTY_1.I6")
        self.add_children_to_concept("EFFECT_CERTAINTY.I6","EFFECT_CERTAINTY_2.I6")
        self.add_children_to_concept("EFFECT_CERTAINTY.I6","EFFECT_CERTAINTY_3.I6")
        self.add_children_to_concept("EFFECT_CERTAINTY.I6","EFFECT_CERTAINTY_4.I6")
        self.add_children_to_concept("EFFECT_CERTAINTY.I6","EFFECT_CERTAINTY_5.I6")

        self.add_children_to_concept("FUNCTION_TYPE.I4","SITE_FUNCTION_TYPE.I4")
        self.add_children_to_concept("FUNCTION_CERTAINTY.I6","SITE_FUNCTION_CERTAINTY.I6")

    def load_json_resources(self, input_file, remove_ids=False):

        with open(input_file, "rb") as openf:
            data = json.loads(openf.read())
        resources = data['resources']

        # justusethese = [
            # "5ec1ce35-2dcd-4b5b-8094-36f3e81349c7",
            # "ebf8d2ba-c707-4718-8ecf-2d0c8fb186bb",
            # "5fb1b6ca-4a64-406e-9e92-3d74e30e1188",
            # "77a99c67-1494-4239-997e-5b054494c124"
        # ]
        # resources = [i for i in resources if i['entityid'] in justusethese]

        if remove_ids is True:
            for resource in resources:
                branches = [b for b in resource['child_entities'] if not\
                    b['entitytypeid'] == "EAMENA_ID.E42"]
                resource['child_entities'] = branches

        return resources

    def convert_v1_json(self, input_file, slice=None, remove_ids=False):

        extended_date_ct = 0
        print "preparing node name and label lookups..."
        nl = self.load_lookup()
        ll = self.make_full_label_lookup("HERITAGE_PLACE.E27")
        cpl = self.load_cultural_period_lookup()
        lt = self.load_label_transformations()
        al = self.load_assessor_lookup()

        print "   done."

        extended_date_resources = list()
        all_assessor_uuids = list()

        # original code used to parse the normal json file.
        if input_file.endswith(".json"):
            outrows = list()
            resources = self.load_json_resources(input_file, remove_ids=remove_ids)

            # slice list if desired
            if not slice:
                s, e = None, None
            else:
                s, e = slice.split(":")
                s = int(s) if s != "" else None
                e = int(e) if e != "" else None

            use_these = resources[s:e]
            allids = [i['entityid'] for i in use_these]
            print "converting {} resources".format(len(use_these))

            errors = list()
            missing_labels = list()
            for resource in use_these:
                if self.verbose:
                    print "=== new resource ==="
                res = NewResource(resource,
                    node_lookup=nl,
                    label_lookup=ll,
                    period_lookup=cpl,
                    label_transformations=lt,
                    assessor_lookup=al
                )
                if self.verbose:
                    print res.resid
                res.make_rows()
                outrows += res.rows

                if res.has_extended_dates:
                    extended_date_ct += 1
                if len(res.errors) > 0:
                    errors.append("Resource ID: {}".format(res.resid))
                    errors += res.errors
                missing_labels += res.missing_labels

            out_arches = input_file.replace(".json","-v2.arches")
            self.write_arches_file(outrows, outname=out_arches)

        # new and improved code to handle jsonl files
        elif input_file.endswith(".jsonl"):
            allids = list()
            errors = list()
            missing_labels = list()
            out_arches = input_file.replace(".jsonl","-v2.arches")
            with open(out_arches, "wb") as openout:
                writer = unicodecsv.writer(openout, delimiter="|")
                writer.writerow(['RESOURCEID', 'RESOURCETYPE', 'ATTRIBUTENAME', 'ATTRIBUTEVALUE', 'GROUPID'])
                with open(input_file, "rb") as openin:
                    lines = openin.readlines()

                    for line in lines:
                        resource = json.loads(line)
                        allids.append(resource['entityid'])
                        if self.verbose:
                            print "=== new resource ==="
                        res = NewResource(resource,
                            node_lookup=nl,
                            label_lookup=ll,
                            period_lookup=cpl,
                            label_transformations=lt
                        )
                        if self.verbose:
                            print res.resid
                        res.make_rows()
                        for row in res.rows:
                            writer.writerow(row)

                        if res.has_extended_dates:
                            extended_date_resources.append(res)
                        for assessor in res.assessor_uuids:
                            all_assessor_uuids.append(assessor)
                        for err in res.errors:
                            errors.append("{} {}".format(res.resid, err))
                        missing_labels += res.missing_labels

        orig_relations = os.path.splitext(input_file)[0] + "_resource_relationships.csv"
        out_relations = out_arches.replace(".arches",".relations")
        self.convert_or_create_relations(orig_relations, out_relations, resids=allids)
        
        if len(extended_date_resources) > 0:
            exdaterows = list()
            for r in extended_date_resources:
                for rr in r.rows:
                    exdaterows.append(rr)
            self.write_arches_file(exdaterows, outname="extended_date_resources.arches")

        if len(all_assessor_uuids) > 0:
            with open("assessor_uuids.csv", "wb") as opena:
                opena.write("assessor id\n")
                for au in set(all_assessor_uuids):
                    opena.write(au+"\n")

        if len(errors) > 0:
            with open(logfile, "wb") as openf:
                for e in errors:
                    openf.write(e.encode("utf-8")+os.linesep)
            print "errors written to", logfile

        if len(missing_labels) > 0:
            missed = sorted(list(set(missing_labels)))
            with open(missed_labels_file, "wb") as openf:
                writer = unicodecsv.writer(openf)
                writer.writerow(['node','invalid_label'])
                for row in missed:
                    writer.writerow(row)
            print "summary of missing labels written to ", missed_labels_file

        if not missing_labels and not errors:
            print "\n--- completed without errors(!) ---"

    def convert_or_create_relations(self, source_relations, out_relations, resids=None):

        relations = []
        if os.path.isfile(source_relations):
            with open(source_relations, "rb") as openorig:
                reader = unicodecsv.reader(openorig)
                reader.next()
                for row in reader:
                    if resids is not None:
                        if row[0] not in resids or row[1] not in resids:
                            continue
                    relations.append(row)
            if self.verbose:
                print "using existing relations file"
                print "  {} relations".format(len(relations))
        else:
            if self.verbose:
                print "ceating blank relations file"

        # check and convert concept ids if that's what's in the rr file
        for row in relations:
            rtype = row.pop(4)
            try:
                concept = Concepts.objects.get(conceptid=rtype)
                value = Values.objects.filter(conceptid=concept)[0].valueid
                row.insert(4, value)
            except Exception as e:
                row.insert(4, rtype)

        with open(out_relations, "wb") as openf:
            writer = unicodecsv.writer(openf, delimiter="|")
            writer.writerow(['RESOURCEID_FROM', 'RESOURCEID_TO', 'START_DATE', 'END_DATE', 'RELATION_TYPE', 'NOTES'])
            for row in relations:
                writer.writerow(row)

    def write_arches_file(self, rows, outname="v2_resources.arches"):

        with open(outname, "wb") as openf:
            writer = unicodecsv.writer(openf, delimiter="|")
            writer.writerow(['RESOURCEID', 'RESOURCETYPE', 'ATTRIBUTENAME', 'ATTRIBUTEVALUE', 'GROUPID'])
            for row in rows:
                writer.writerow(row)

    def load_cultural_period_lookup(self):

        lookupdir = os.path.join(settings.ROOT_DIR,"app","utils","v1v2lookups")
        f = os.path.join(lookupdir, "Cultural_Period_v1-v2_Translation.csv")
        lookup = dict()
        with open(f, "rb") as openf:
            reader = unicodecsv.reader(openf)
            reader.next()
            for row in reader:
                lookup[row[0]] = {"cp":row[1],"sp":row[2]}

        return lookup

    def load_label_transformations(self):

        lookupdir = os.path.join(settings.ROOT_DIR,"app","utils","v1v2lookups")
        f = os.path.join(lookupdir, "General_Label_Transformations.csv")
        lookup = dict()
        with open(f, "rb") as openf:
            reader = unicodecsv.reader(openf)
            reader.next()
            for row in reader:
                old, new = row[1].lower().rstrip(), row[2].lower().rstrip()
                if not row[0] in lookup:
                    lookup[row[0]] = {old:new}
                else:
                    lookup[row[0]][old] = new

        return lookup

    def load_lookup(self):

        lookupdir = os.path.join(settings.ROOT_DIR,"app","utils","v1v2lookups")
        f = os.path.join(lookupdir, "HERITAGE_RESOURCE_GROUP.E27_NodeNames.csv")
        lookup = dict()
        with open(f, "rb") as openf:
            reader = unicodecsv.reader(openf)
            reader.next()
            for row in reader:
                if row[1] == "":
                    lookup[row[0]] = row[0]
                else:
                    lookup[row[0]] = row[1]

        return lookup

    def load_assessor_lookup(self):

        lookupdir = os.path.join(settings.ROOT_DIR,"app","utils","v1v2lookups")
        f = os.path.join(lookupdir, "MainDB_Assessor_Lookup.csv")
        lookup = dict()
        with open(f, "rb") as openf:
            reader = unicodecsv.reader(openf)
            reader.next()
            for row in reader:
                lookup[row[0]] = row[1]

        return lookup

    def make_full_label_lookup(self, restype):

        q = Entity().get_mapping_schema(restype)
        restypenodes = set(q.keys())

        outdict = {}
        for node_name in restypenodes:
            node_obj = EntityTypes.objects.get(pk=node_name)

            if node_obj.businesstablename == "domains":
                outdict[node_name] = self.get_label_lookup(node_obj.conceptid_id)

        with open("full_label_lookup.json", 'wb') as out:
            json.dump(outdict, out, indent=1)
        return outdict

    def get_label_lookup(self, conceptid, return_entity=False):

        all_concepts = self.collect_concepts(conceptid,full_concept_list=[])

        ## dictionary will hold {label:concept.legacyoid} or {label:valueid}
        label_lookup = {}
        for c in all_concepts:
            cobj = Concepts.objects.get(pk=c)
            labels = Values.objects.filter(conceptid_id=c,valuetype_id="prefLabel")
            for label in labels:
                if return_entity:
                    label_lookup[label.value.lower()] = label.valueid
                else:
                    label_lookup[label.value.lower()] = cobj.legacyoid

        return label_lookup

    def collect_concepts(self, node_conceptid, full_concept_list = []):
        ''' Collects a full list of child concepts given the conceptid of the node.
        Returns a list of a set of concepts, i.e. expounding the duplicates'''
        concepts_in_node = ConceptRelations.objects.filter(conceptidfrom = node_conceptid)
        if concepts_in_node.count() > 0:
            full_concept_list.append(node_conceptid)
            for concept_in_node in concepts_in_node:
                self.collect_concepts(concept_in_node.conceptidto_id, full_concept_list)
        else:
            full_concept_list.append(node_conceptid)
        return list(set(full_concept_list))
