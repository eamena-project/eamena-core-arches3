import os
import uuid
import json
import csv
import datetime
from time import time
from django.conf import settings
from django.db import connection, transaction
from django.contrib.auth.models import User
from django.forms.models import model_to_dict
from django.core.management.base import BaseCommand, CommandError
from arches.app.models.entity import Entity
from arches.app.models.resource import Resource
from arches.app.models.models import Concepts
from arches.app.models.models import Values
from arches.app.models.models import RelatedResource
from arches.app.models.models import Entities, UniqueIds
from arches.app.models.concept import Concept
from arches.app.search.search_engine_factory import SearchEngineFactory
from arches.management.commands import utils
from optparse import make_option
from formats.archesfile import ArchesReader
from formats.archesjson import JsonReader
from formats.shpfile import ShapeReader
from django.core.exceptions import ObjectDoesNotExist
from arches.app.utils.eamena_utils import make_load_id

import logging
logger = logging.getLogger(__name__)

# def resource_list_chunk_to_entities_wrapper(args):
    # return ResourceLoader.resource_list_chunk_to_entities(*args)

class ResourceLoader(object):

    def __init__(self):
        self.user = User()
        self.user.first_name = settings.ETL_USERNAME
        self.resources = []
        self.se = SearchEngineFactory().create()

    option_list = BaseCommand.option_list + (
        make_option('--source',
            action='store',
            dest='source',
            default='',
            help='.arches file containing resource records'),
         make_option('--format',
            action='store_true',
            default='arches',
            help='format extension that you would like to load: arches or shp'),
        )

    def load(self, source, appending = False, load_id=None):
        d = datetime.datetime.now()

        if load_id is None:
            load_id = 'LOADID:{0}-{1}-{2}-{3}-{4}-{5}'.format(d.year, d.month, d.day,
                d.hour, d.minute, d.microsecond)

        file_name, file_format = os.path.splitext(source)
        archesjson = False
        if file_format == '.shp':
            reader = ShapeReader()
        elif file_format == '.arches':
            reader = ArchesReader()
            print '\nVALIDATING ARCHES FILE ({0})'.format(source)
            # reader.validate_file(source)
        elif file_format == '.json':
            archesjson = True
            reader = JsonReader()
            print '\nVALIDATING JSON FILE ({0})'.format(source)
            reader.validate_file(source)
        elif file_format == '.jsonl':
            archesjson = True
            reader = JsonReader()
            print '\nNO VALIDATION USED ON JSONL FILE ({0})'.format(source)
            loaded_ct = 0
            with open(source, "rb") as openf:
                lines = openf.readlines()
                for line in lines:
                    resource = json.loads(line)
                    result = self.resource_list_to_entities([resource], load_id, True, False,
                        filename=os.path.basename(source))
                    loaded_ct += 1
            return {"count":loaded_ct}

        start = time()
        resources = reader.load_file(source)

        print '\nLOADING RESOURCES ({0})'.format(source)
        relationships = None
        related_resource_records = []
        relationships_file = file_name + '.relations'
        elapsed = (time() - start)
        print 'time to parse {0} resources = {1}'.format(file_name, elapsed)
        results = self.resource_list_to_entities(resources, load_id, archesjson, appending,
            filename=os.path.basename(source))
        if os.path.exists(relationships_file):
            with open(relationships_file, "rb") as openf:
                lines = openf.readlines()
                if "," in lines[0]:
                    delim = ","
                elif "|" in lines[0]:
                    delim = "|"
                else:
                    delim = ","
            relationships = csv.DictReader(open(relationships_file, 'r'), delimiter=delim)
            for relationship in relationships:
                related_resource_records.append(self.relate_resources(relationship, results['legacyid_to_entityid'], archesjson))
        else:
            print 'No relationship file'

        return results

        #self.se.bulk_index(self.resources)
    
    # def resource_list_chunk_to_entities():
        


    def resource_list_to_entities(self, resource_list, load_id, archesjson=False, append=False, filename=''):
        '''Takes a collection of imported resource records and saves them as arches entities'''
        start = time()
        d = datetime.datetime.now()

        ret = {'successfully_saved':0, 'failed_to_save':[], 'load_id': load_id}
        schema = None
        current_entitiy_type = None
        legacyid_to_entityid = {}
        errors = []
        progress_interval = 250
        
        def chunks(l, n):
            """Yield successive n-sized chunks from l. Thanks to:
            https://stackoverflow.com/a/312464/3873885"""
            for i in xrange(0, len(l), n):
                yield l[i:i + n]

        elapsed = 0
        chunktimes = list()
        for m, resource_list_chunk in enumerate(chunks(resource_list, progress_interval)):
            startchunk = time()
            multiplier = m + 1
            with transaction.atomic():
                for count, resource in enumerate(resource_list_chunk):
                    real_ct = count + 1
                    if archesjson == False:
                        masterGraph = None
                        if current_entitiy_type != resource.entitytypeid:
                            schema = Resource.get_mapping_schema(resource.entitytypeid)
                            current_entitiy_type = resource.entitytypeid

                        master_graph = self.build_master_graph(resource, schema)
                        self.pre_save(master_graph)

                        try:
                            uuid.UUID(resource.resource_id)
                            entityid = resource.resource_id
                        except ValueError:
                            entityid = ''

                        if append:
                            try:
                                resource_to_delete = Resource(entityid)
                                resource_to_delete.delete_index()
                            except ObjectDoesNotExist:
                                print 'Entity ',entityid,' does not exist. Nothing to delete'
                        
                        try:
                            master_graph.save(user=self.user, note=load_id, resource_uuid=entityid)
                        except Exception as e:
                            logger.warn( 'Could not save resource {}.\nERROR: {}'.format(entityid,e))
                            print 'Could not save resource {}.\nERROR: {}'.format(entityid,e)
                        resource.entityid = master_graph.entityid
                        #new_resource = Resource().get(resource.entityid)
                        #assert new_resource == master_graph
                        try:
                            master_graph.index()
                            full_resource = Resource().get(resource.entityid)
                            full_resource.index()
                        except Exception as e:
                            logger.warn('Could not index resource {}.\nERROR: {}'.format(resource.entityid,e))
                        legacyid_to_entityid[resource.resource_id] = master_graph.entityid
                    else:
                        new_resource = Resource(resource)
                        try:
                            new_resource.save(user=self.user, note=load_id, resource_uuid=new_resource.entityid)
                        except Exception as e:
                            print 'Could not save resource {}.\nERROR: {}'.format(resource['entityid'],e)
                            # with open(resource['entityid']+".json", "wb") as f:
                                # json.dump(resource, f, indent=1)
                            continue
                        new_resource = Resource().get(new_resource.entityid)
                        try:
                            new_resource.index()
                        except Exception as e:
                            print 'Could not index resource {}.\nERROR: {}'.format(resource.entityid,e)
                        legacyid_to_entityid[new_resource.entityid] = new_resource.entityid

                    ret['successfully_saved'] += 1
            endchunk = time() - startchunk

            chunktimes.append(endchunk)
            chunktime_avg = sum(chunktimes)/len(chunktimes)
            remtime = ((len(resource_list) - (multiplier*progress_interval))*chunktime_avg/progress_interval)/60
            if real_ct == progress_interval:
                print "{} of {} loaded in {}m. remaining time estimate: {}m".format(
                    progress_interval*multiplier, len(resource_list), round(sum(chunktimes)/60, 2),
                    round(remtime, 2))

            else:
                print progress_interval*multiplier+real_ct

        ret['legacyid_to_entityid'] = legacyid_to_entityid
        elapsed = (time() - start)
        print len(resource_list), 'resources loaded'
        if len(resource_list) > 0:
            print 'total time to etl = %s' % (elapsed)
            print 'average time per entity = %s' % (elapsed/len(resource_list))
            print 'Load Identifier =', load_id
            print '***You can reverse this load with the following command:'
            print 'python manage.py packages -o remove_resources --load_id', load_id
            log_msg = "\n~~~~~\n{}\nfile: {}\nresources: {}\nloadid: {}".format(
                d.strftime("%d/%m/%Y - %H:%M"),filename,len(resource_list),load_id
            )
            with open(settings.BULK_UPLOAD_LOG_FILE, "a") as loadlog:
                loadlog.write(log_msg)
        return ret

    def build_master_graph(self, resource, schema):
        master_graph = None
        entity_data = []

        if len(entity_data) > 0:
            master_graph = entity_data[0]
            for mapping in entity_data[1:]:
                master_graph.merge(mapping)

        for group in resource.groups:
            entity_data2 = []
            for row in group.rows:
                entity = Resource()
                entity.create_from_mapping(row.resourcetype, schema[row.attributename]['steps'], row.attributename, row.attributevalue)
                entity_data2.append(entity)  

            mapping_graph = entity_data2[0]
            for mapping in entity_data2[1:]:
                mapping_graph.merge(mapping)

            if master_graph == None:
                master_graph = mapping_graph
            else:
                node_type_to_merge_at = schema[row.attributename]['mergenodeid']
                has_merge_in_path = 0
                new_merge_node = None
                for ent in entity_data2:
                    for step in ent.flatten():
                        if step.entitytypeid == node_type_to_merge_at:
                            has_merge_in_path += 1
                            break
                for ent in mapping_graph.flatten():
                    if ent.entitytypeid == node_type_to_merge_at and ent.value != '':
                        new_merge_node = schema[node_type_to_merge_at]['mergenodeid']
                if has_merge_in_path != len(entity_data2):
                    # Merge node is not in path of each node - so will merge in at root.
                    master_graph.merge_at(mapping_graph, mapping_graph.entitytypeid)
                elif new_merge_node:
                    # Merge node is a value node - so will merge one node up
                    master_graph.merge_at(mapping_graph, new_merge_node)
                else:
                    master_graph.merge_at(mapping_graph, node_type_to_merge_at)
        return master_graph

    def pre_save(self, master_graph):
        pass

    def relate_resources(self, relationship, legacyid_to_entityid, archesjson):
        start_date = None if relationship['START_DATE'] in ('', 'None') else relationship['START_DATE']
        end_date = None if relationship['END_DATE'] in ('', 'None') else relationship['END_DATE']

        if archesjson == False:
            relationshiptype_concept = Concepts.objects.get(legacyoid = relationship['RELATION_TYPE'])
            concept_value = Values.objects.filter(conceptid = relationshiptype_concept.conceptid).filter(valuetype = 'prefLabel')
            entityid1 = legacyid_to_entityid[relationship['RESOURCEID_FROM']]
            if relationship['RESOURCEID_TO'] in legacyid_to_entityid.keys():
                entityid2 = legacyid_to_entityid[relationship['RESOURCEID_TO']]
            else:
                # If entityid is not in dictionary, likely is a uuid to previously existing resource
                entityid2 = relationship['RESOURCEID_TO']

        else:
            concept_value = Values.objects.filter(valueid = relationship['RELATION_TYPE'])
            entityid1 = relationship['RESOURCEID_FROM']
            entityid2 = relationship['RESOURCEID_TO']

        if len(concept_value) == 0:
            concept = Concepts.objects.get(conceptid=relationship['RELATION_TYPE'])
            concept_value = Values.objects.filter(conceptid=concept)

        related_resource_record = RelatedResource(
            entityid1 = entityid1,
            entityid2 = entityid2,
            notes = relationship['NOTES'],
            relationshiptype = concept_value[0].valueid,
            datestarted = start_date,
            dateended = end_date
            )

        related_resource_record.save()
        self.se.index_data(index='resource_relations', doc_type='all', body=model_to_dict(related_resource_record), idfield='resourcexid')
