import os
from optparse import make_option
from django.core.management.base import BaseCommand, CommandError
from arches.app.utils.make_label_lookup import generate_lookup

class Command(BaseCommand):

    option_list = BaseCommand.option_list + (
        make_option('-d', '--destination', action='store', default=None,
            help="path output file"),
    )

    def handle(self, *args, **options):

        if options['destination'] is None:
            outpath = "label_x_concept_legacyoid.json"

        else:
            outpath = os.path.realpath(options['destination'])

        print "output file:", outpath

        generate_lookup(outpath)
