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
        make_option('--verbose', action='store_true', default=False,
            help="enables extra print statements")
    )

    def handle(self, *args, **options):

        self.update_concept_uuids(options['source'],slice=options['slice'])

    def load_json_resources(self, input_file):

        with open(input_file, "rb") as openf:
            data = json.loads(openf.read())
        resources = data['resources']

        return resources

    def process_one_resource(self,resource,label_lookup={},label_transformations={}):
        
        def process_entities(entities):
            
            for entity in entities:
                if entity['businesstablename'] == "domains":
                    label = entity['label'].lower().rstrip()
                    entitytype = entity['entitytypeid']
                    if entitytype in label_transformations:
                        if label in label_transformations[entitytype]:
                            label = label_transformations[entitytype][label]
                    if entitytype in label_lookup:
                        if label in label_lookup[entitytype]:
                            newid = label_lookup[entitytype][label].split("/")[-1]
                            entity['value'] = newid
                        elif label == "resized":
                            newid = label_lookup[entitytype]["other/unlisted"].split("/")[-1]
                            entity['value'] = newid
                        else:
                            print "missing label:", label
                            for k,v in label_lookup[entitytype].items():
                                print k,v
                
                if len(entity['child_entities']) > 0:
                    process_entities(entity['child_entities'])
            
            return entities
        
        processed = process_entities([resource])

        return processed

    def update_concept_uuids(self, input_file, slice=None, remove_ids=False):

        print "preparing node name and label lookups..."
        ll = self.make_full_value_lookup("INFORMATION_RESOURCE.E73")
        lt = self.load_label_transformations()
        
        # print lt
        # exit()

        print "   done."
        
        # original code used to parse the normal json file.
        
        if input_file.endswith(".json"):
            raise Exception("this command can't yet handle JSON files. only JSONL.")
        
        # new and improved code to handle jsonl files
        elif input_file.endswith(".jsonl"):
            allids = list()
            errors = list()
            missing_labels = list()
            out_file = input_file.replace(".jsonl","-revised.jsonl")
            with open(out_file, "wb") as openout:
                with open(input_file, "rb") as openin:
                    lines = openin.readlines()

                    for line in lines:
                        if self.verbose:
                            print "=== new resource ==="
                        resource = json.loads(line)
                        if self.verbose:
                            print resource['entityid']

                        newres = self.process_one_resource(resource,
                            label_lookup=ll, label_transformations=lt
                        )[0]
                        # print newres
                        openout.write(json.dumps(newres, openout) + "\n")
                        # break


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

    def make_full_value_lookup(self, restype):

        q = Entity().get_mapping_schema(restype)
        restypenodes = set(q.keys())

        outdict = {}
        for node_name in restypenodes:
            node_obj = EntityTypes.objects.get(pk=node_name)
            if node_obj.businesstablename == "domains":
                outdict[node_name] = self.get_label_lookup(node_obj.conceptid_id, return_entity=True)
        with open("full_label_lookup.json", 'wb') as out:
            json.dump(outdict, out, indent=1)
        return outdict

    def get_label_lookup(self, conceptid, return_entity=False):

        all_concepts = self.collect_concepts(conceptid,full_concept_list=[])
        # print " ", len(all_concepts)
        ## dictionary will hold {label:concept.legacyoid} or {label:valueid}
        label_lookup = {}
        for c in all_concepts:
            cobj = Concepts.objects.get(pk=c)
            labels = Values.objects.filter(conceptid_id=c,valuetype_id="prefLabel")
            for label in labels:
                # try:
                    # print label.value
                # except Exception as e:
                    # print str(e)
                if return_entity:
                    label_lookup[label.value.lower()] = label.valueid
                else:
                    label_lookup[label.value.lower()] = cobj.legacyoid

        return label_lookup

    def collect_concepts(self, node_conceptid, full_concept_list = []):
        ''' Collects a full list of child concepts given the conceptid of the node. Returns a list of a set of concepts, i.e. expounding the duplicates'''
        concepts_in_node = ConceptRelations.objects.filter(conceptidfrom = node_conceptid)
        # if node_conceptid == "4e86ecd2-5d33-11e9-9dec-5fb62f82c54b":
            # print len(concepts_in_node)
        if concepts_in_node.count() > 0:
            full_concept_list.append(node_conceptid)
            for concept_in_node in concepts_in_node:
                # full_concept_list.append(concept_in_node.conceptidto_id)
                self.collect_concepts(concept_in_node.conceptidto_id, full_concept_list)
        else:
            # print node_conceptid
            full_concept_list.append(node_conceptid)
        return list(set(full_concept_list))
