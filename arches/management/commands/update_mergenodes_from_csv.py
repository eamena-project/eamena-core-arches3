import os
import unicodecsv
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
from arches.app.models.models import Mappings

class Command(BaseCommand):

    option_list = BaseCommand.option_list + (
        make_option('-s', '--source', action='store', default=None,
            help="path to nodes csv"),
    )

    def handle(self, *args, **options):

        if options['source'] is None:
            print "invalid source."
            exit()

        else:
            if not os.path.isfile(options['source']):
                print "invalid source."
                exit()
            filepath = options['source']

        self.update_mappings(filepath)

    def update_mappings(self,filepath):

        node_data = {}
        entitytypeid = None
        with open(filepath, "rb") as openf:
            reader = unicodecsv.reader(openf)
            reader.next()
            for row in reader:
                nodename = row[1]
                mergenode = row[2]
                if nodename == mergenode:
                    entitytypeid = nodename
                else:
                    node_data[nodename] = mergenode
        
        if entitytypeid is None:
            print "invalid nodes file"
            exit()
        else:
            print "--", entitytypeid, "--"

        for node, mergenode in node_data.iteritems():
            mapping = Mappings.objects.get(entitytypeidfrom=entitytypeid,entitytypeidto=node)
            
            if mapping.mergenodeid != mergenode:
                print "{} altering mergenode".format(node)
                print "  {} --> {}".format(mapping.mergenodeid, mergenode)
                mapping.mergenodeid = mergenode
                mapping.save()