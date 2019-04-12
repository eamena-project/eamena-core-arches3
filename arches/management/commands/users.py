import unicodecsv
from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
from arches.app.utils import auth_system

class Command(BaseCommand):

    option_list = BaseCommand.option_list + (
        make_option('-o', '--operation', action='store', choices=["export","load"],
            help="choose whether to export or import users"),
        make_option('-s', '--source', action='store', default=None,
            help="path to import file"),
        make_option('-d', '--destination', action='store', default=None,
            help="path to export file"),
        make_option('--overwrite', action='store_true', default=False,
            help="specify whether existing users should be overwritten by "\
            "those in the input file if the usernames match"),
    )

    def handle(self, *args, **options):

        if options['operation'] == "export":
            if options['destination'] is None:
                print "\nYou must specify a destination file name using\n\n"\
                    "    -d/--destination\n"
                exit()

            auth_system.export_users(options['destination'])
            print "WARNING: raw passwords cannot be exported, so new passwords have "\
            "been created for each user."
            print "\noutput file: {}".format(options['destination'])

        if options['operation'] == "load":
            if options['source'] is None:
                print "\nYou must specify a source file using\n\n"\
                    "    -s/--source\n"
                exit()

            auth_system.load_users(options['source'], overwrite=options['overwrite'])
