import unicodecsv
import uuid
from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
from arches.app.models.models import Values, Concepts, RelatedResource

# logdir = os.path.join(settings.PACKAGE_ROOT,"logs")
# logfile = os.path.join(logdir,"v1_conversion_general_log.txt")
# missed_labels_file = os.path.join(logdir,"v1_conversion_missing_labels.csv")

class Command(BaseCommand):

    verbose = False

    option_list = BaseCommand.option_list + (
        make_option('-c', '--concepts', action='store_true',
            help="write out the concept ids instead of value ids"),
    )

    def handle(self, *args, **options):

        rrs = RelatedResource.objects.all()
        print rrs.count()
        with open("all_relations.relations", "wb") as openf:
            writer = unicodecsv.writer(openf, delimiter="|")
            writer.writerow(['RESOURCEID_FROM','RESOURCEID_TO','START_DATE','END_DATE','RELATION_TYPE','NOTES'])
            for rr in rrs:
                if options['concepts']:
                    rtype = Values.objects.get(valueid=rr.relationshiptype).conceptid
                else:
                    rtype = rr.relationshiptype
                row = [rr.entityid1, rr.entityid2, rr.datestarted, rr.dateended, rtype, rr.notes]
                writer.writerow(row)