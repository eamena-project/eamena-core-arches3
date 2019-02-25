import os
import json
import unicodecsv
import uuid
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
from arches.app.models.models import Concepts
from arches.app.models.concept import Concept
from arches.app.models.models import ConceptRelations

from arches.app.utils.skos import SKOSReader

class Command(BaseCommand):

    option_list = BaseCommand.option_list + (
        # make_option('-o', '--operation', action='store',
            # type='choice', 
            # choices=['load','link'],
            # help="path to skosfile"),
        make_option('-d', '--destination', action='store', default=None,
            help="full path output file"),
    )

    def handle(self, *args, **options):

        if options['destination'] is None:
            filepath = "dropdown_contents.json"
        else:
            filepath = options['destination']

        self.export_dropdowns(filepath)

    def export_dropdowns(self,filepath):

        print "exporting dropdowns"
        full_contents = dict()
        
        def collect_relations(concept_relation,output=[]):
            
            descendants = ConceptRelations.objects.filter(
                conceptidfrom_id=concept_relation.conceptidto_id,
                relationtype_id="member"
            )
            for child in descendants:
                output.append({
                    "from": child.conceptidfrom_id,
                    "to": child.conceptidto_id,
                    "type": child.relationtype_id
                })
                collect_relations(child, output=output)
            
            return output

        dropdown_relations = ConceptRelations.objects.filter(
            conceptidfrom_id="00000000-0000-0000-0000-000000000003"
        )

        for top_relation in dropdown_relations:

            collection_concept = Concept().get(id=top_relation.conceptidto_id)
            dd_name = collection_concept.values[0].value

            relations = collect_relations(top_relation,output=[])
            full_contents[dd_name] = relations

        for k in sorted(full_contents):
            print "{}: {} members".format(k, len(full_contents[k]))
        print len(full_contents), "collections"
        with open(filepath,"wb") as out:
            json.dump(full_contents,out,indent=1)

        return
