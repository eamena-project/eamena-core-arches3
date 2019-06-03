
import os
import types
import sys
import datetime
from django.conf import settings
from django.db import connection
import arches.app.models.models as archesmodels
from arches.app.models.resource import Resource
from arches.app.models.models import UniqueIds
import codecs
from format import Writer
import json
from arches.app.utils.betterJSONSerializer import JSONSerializer, JSONDeserializer
import time
from arches.management.commands import utils

import time
from multiprocessing import Pool, TimeoutError, cpu_count
from django.db import connections

# this wrapper function must be outside of the class to be called during the
# multiprocessing operations.
def write_one_resource_wrapper(args):
    return JsonWriter.write_one_resource(*args)

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

class JsonWriter(Writer):

    def __init__(self, jsonl=False):
        super(JsonWriter, self).__init__()
        self.jsonl = jsonl

    def write_one_resource(self, resource_id):

        a_resource = Resource().get(resource_id)
        a_resource.form_groups = None
        jsonres = JSONSerializer().serialize(a_resource, separators=(',',':'))
        return jsonres

    def write_resources(self, resources, resource_export_configs):
    
        # Introduce a new section here to export jsonl files. This is for huge databases,
        # and can only be called through the commandline.
        if self.jsonl is True:
            dest_dir = resources
            process_count = cpu_count()
            print "number of cores:", cpu_count()
            print "number of parallel processes:", process_count
            pool = Pool(cpu_count())

            restypes = [i.entitytypeid for i in archesmodels.EntityTypes.objects.filter(isresource=True)]

            for restype in restypes:
                start = time.time()
                resources = archesmodels.Entities.objects.filter(entitytypeid=restype)
                resids = [r.entityid for r in resources]
                
                for conn in connections.all():
                    conn.close()
                
                outfile = dest_dir.replace(".jsonl","-"+restype+".jsonl")
                with open(outfile, 'w') as f:

                    print "Writing {0} {1} resources".format(len(resids), restype) 
                    joined_input = [(self,r) for r in resids]
                    for res in pool.imap(write_one_resource_wrapper, joined_input):
                        f.write(res+"\n")

                print "elapsed time:", time.time()-start
            return

        # this is a hacky way of fixing this method, as the real problem lies
        # lies upstream where this method is called.

        # If the resources variable is a string, assume that it's been passed
        # in through the command line, and is actually the file path/name of
        # the desired output json file. In this case, run the original v3 code here.
        elif isinstance(resources, str):
            dest_dir = resources
            cursor = connection.cursor()
            cursor.execute("""select entitytypeid from data.entity_types where isresource = TRUE""")
            resource_types = cursor.fetchall()
            json_resources = []
            with open(dest_dir, 'w') as f:
                for resource_type in resource_types:
                    resources = archesmodels.Entities.objects.filter(entitytypeid = resource_type)
                    print "Writing {0} {1} resources".format(len(resources), resource_type[0])
                    errors = []
                    for resource in resources:
                        try:
                            a_resource = Resource().get(resource.entityid)
                            a_resource.form_groups = None
                            json_resources.append(a_resource)
                        except Exception as e:
                            if e not in errors:
                                errors.append(e)
                    if len(errors) > 0:
                        print errors[0], ':', len(errors)
                f.write((JSONSerializer().serialize({'resources':json_resources}, separators=(',',':'))))
            return
        # if resources is not a str, then assume it has been passed in through the
        # search UI instantiation of this command. In this case, run the EAMENA
        # export logic
        else:
            json_resources = []
            json_resources_for_export = []
            iso_date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            json_file_name = os.path.join('{0}_{1}.{2}'.format('EAMENA', iso_date, 'json'))
            f = StringIO()

            for count, resource in enumerate(resources, 1):
                if count % 1000 == 0:
                    print "%s Resources exported" % count            
                errors = []

                try:
                    a_resource = Resource().get(resource['_id'])

                    a_resource.form_groups = None
                    json_resources.append(a_resource)
                except Exception as e:
                    if e not in errors:
                        errors.append(e)
            if len(errors) > 0:
                print errors[0], ':', len(errors)

            f.write((JSONSerializer().serialize({'resources':json_resources}, indent = 4, separators=(',',':'))))
            json_resources_for_export.append({'name': json_file_name, 'outputfile': f})
            return json_resources_for_export

class JsonReader():

    def validate_file(self, archesjson, break_on_error=True):
        """
        Going to validate the file and return errors similar to arches validation.
        Current only looks at the EAMENA_ID so isn't spun out into a different function/object
        """
        start_time = time.time()
        with open(archesjson, 'r') as f:
            resources = JSONDeserializer().deserialize(f.read())

        errors = []
        for count, resource in enumerate(resources['resources']):
            if resource['entitytypeid'] in settings.EAMENA_RESOURCES:
                id_type = settings.EAMENA_RESOURCES[resource['entitytypeid']]
            else:
                id_type = resource['entitytypeid'].split('_')[0]
            for entity in resource['child_entities']:
                if entity['entitytypeid'] == 'EAMENA_ID.E42':
                    eamena_id = entity['value']
                    num = int(eamena_id[len(id_type) + 1:])
                    found_ents = UniqueIds.objects.filter(val=num, id_type=id_type)
                    if len(found_ents) > 0:
                        errors.append('ERROR RESOURCE: {0} - {1} is a pre-existing unique ID.'.format(count+1, eamena_id))
                        break

        duration = time.time() - start_time
        print 'Validation of your JSON file took: {0} seconds.'.format(str(duration))

        if len(errors) > 0:
            utils.write_to_file(os.path.join(settings.PACKAGE_ROOT, 'logs', 'validation_errors.txt'),
                                '\n'.join(errors))
            print "\n\nERROR: There were errors detected in your JSON file."
            print "Please review the errors at %s, \ncorrect the errors and then rerun this script." % (
                os.path.join(settings.PACKAGE_ROOT, 'logs', 'validation_errors.txt'))
            if break_on_error:
                sys.exit(101)

    def load_file(self, archesjson):
        resources = []
        with open(archesjson, 'r') as f:
            resources = JSONDeserializer().deserialize(f.read())
        
        return resources['resources']