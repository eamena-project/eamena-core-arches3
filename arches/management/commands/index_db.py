'''
ARCHES - a program developed to inventory and manage immovable cultural heritage.
Copyright (C) 2013 J. Paul Getty Trust and World Monuments Fund

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

"""This module contains commands for building Arches."""

from optparse import make_option
from django.core.management.base import BaseCommand, CommandError
from arches.app.utils.index_database import index_db

class Command(BaseCommand):
    """A general command used index Arches data into Elasticsearch."""

    option_list = BaseCommand.option_list + (
        make_option('-c', '--concepts', action='store_true', default=False,
            help='only index the concepts in your database'),
        make_option('-r', '--resources', action='store_true', default=False,
            help='only index the resources in your database'),
    )

    def handle(self, *args, **options):
        # if both are false, then the whole db should be indexed.
        if options['concepts'] is False and options['resources'] is False:
            index_db()
        elif options['concepts'] is True:
            index_db(resources=False)
        elif options['resources'] is True:
            index_db(concepts=False)
