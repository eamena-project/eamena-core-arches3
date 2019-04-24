import os
import unicodecsv
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
from arches.app.models.models import EntityTypes

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

        self.update_businesstables(filepath)

    def update_businesstables(self,filepath):

        node_data = []
        with open(filepath, "rb") as openf:
            reader = unicodecsv.DictReader(openf)
            reader.next()
            for row in reader:
                nodename = row['Label']
                btn = row['businesstable']
                if btn.rstrip() == "":
                    continue
                node_data.append((nodename,btn))
        node_data.sort()
        ct = 0
        for node, btn in node_data:
            et = EntityTypes.objects.get(entitytypeid=node)
            if et.businesstablename != btn:
                print "{} altering mergenode".format(node)
                print "  {} --> {}".format(et.businesstablename, btn)
                et.businesstablename = btn
                et.save()
                ct += 1
        
        if ct > 0:
            print ct, "businesstablenames updated."
        else:
            print "no businesstablenames updated."
            