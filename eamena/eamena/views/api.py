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

import os
import urllib
from datetime import datetime
from tempfile import NamedTemporaryFile
from django.conf import settings
from django.core.files import File
from django.views.decorators.csrf import csrf_exempt
from django.contrib.gis.geos import GEOSGeometry
from django.forms.models import model_to_dict
from arches.app.utils.betterJSONSerializer import JSONSerializer, JSONDeserializer
from arches.app.utils.JSONResponse import JSONResponse
from arches.app.search.elasticsearch_dsl_builder import Bool, Query, Nested, Terms, GeoShape
from arches.app.search.search_engine_factory import SearchEngineFactory
from arches.app.utils.imageutils import generate_thumbnail
from arches.app.models.resource import Resource
from arches.app.models.entity import Entity


import numpy as np

def create_information_resource(data):

    resid = data['id']
    url = data['url']
    lat = data['latitude']
    lng = data['longitude']
    caption = data['caption']
    epoch_time = data['captureDate']

    yyyymmdd = datetime.fromtimestamp(float(epoch_time)).strftime('%Y-%m-%d')

    ## create resource instance, and set the basic entity values
    res = Resource()
    res.entitytypeid = 'INFORMATION_RESOURCE.E73'
    res.set_entity_value('DATE_OF_ACQUISITION.E50', yyyymmdd)
    res.set_entity_value('URL.E51', url)
    if caption != "":
        res.set_entity_value('DESCRIPTION.E62', caption)

    ## set geometry, but not if 0, 0  is passed in
    if (lat != "" and lng != "") and (lat, lng) != (0,0):
        wkt = "POINT ( {} {} )".format(lng, lat)
        res.set_entity_value('SPATIAL_COORDINATES_GEOMETRY.E47', wkt)

    res.save()

    ## retrieve file from url and save it to the resource
    filename = os.path.basename(url)
    temp_file = os.path.join(settings.MEDIA_ROOT,"temp",filename)
    urllib.urlretrieve(url,temp_file)

    with open(temp_file,"rb") as f:
        django_file = File(f)
        django_file.content_type = "image"
        res.set_entity_value('FILE_PATH.E62', django_file)
        thumb = generate_thumbnail(django_file)
        if thumb != None:
            res.set_entity_value('THUMBNAIL.E62', thumb)
        res.save()

    os.remove(temp_file)

    ## index resource
    res.index()

    #Insert relations
    se = SearchEngineFactory().create()

    ## this is the uuid of the concept value for the only type of relationship
    ## allowed between a HR and an Information resource
    relationship_type = '4f495e34-987a-44bb-ad24-183310f38d82'
    for related_res in data['related_to']:
        relationship = res.create_resource_relationship(related_res, relationship_type_id=relationship_type)
        se.index_data(index='resource_relations', doc_type='all', body=model_to_dict(relationship), idfield='resourcexid')

    response = {
        "created": True,
        "resourceid": res.entityid
    }

    return (response, 201)

@csrf_exempt
def create_resources(request):

    if request.method == 'POST':

        received_json = JSONDeserializer().deserialize(request.body)
        if "" in received_json.values() or None in received_json.values():
            return JSONResponse({'error':'incomplete json'}, status=500)
        
        response,status = create_information_resource(received_json)

    return JSONResponse(response, status=status)

@csrf_exempt
def return_resources(request):
    '''Endpoint to return nearest 1000 EAMENA resources given a GeoJSON bounding box'''
    json_collection = []
    if request.method == 'POST':
        geojson = JSONDeserializer().deserialize(request.body)
        se = SearchEngineFactory().create()
        query= Query(se)
        boolfilter = Bool()
        geoshape = GeoShape(field='geometries.value', type=geojson['type'], coordinates=geojson['coordinates'] )
        nested = Nested(path='geometries', query=geoshape)
        boolfilter.must(nested)
        query.add_query(boolfilter)
        results = query.search(index='entity', doc_type='',start=0, limit = 1000)
        for hit in results['hits']['hits']:
            json_res = {
                'resource_type': '',
                'resource_id': '',
                'resource_name' : '',
                'centroid': ''
                    }
            json_res['resource_type'] = hit['_type']
            json_res['resource_id'] = hit['_id']
            json_res['resource_name'] = hit['_source']['primaryname']
            json_res['centroid'] = generate_centroids(hit['_source']['geometries'])
            json_collection.append(json_res)
    return JSONResponse(json_collection)
        
def generate_centroids(geometries):
    '''In some cases, resources will have multiple geometries. If that is the case, generating one centroid from the first available geometry won't be an accurate representation of the location of the overall resource. In that case, centre points are first generated for each geometry, then the centroid of the centre points is outputted. '''

    geom_list = []
    if geometries:
        if len(geometries) == 1:
            geom = GEOSGeometry(JSONSerializer().serialize(geometries[0]['value']),srid =4326)
            return JSONDeserializer().deserialize(geom.centroid.json)
        else:            
            for geometry in geometries:
                geom = GEOSGeometry(JSONSerializer().serialize(geometry['value']),srid =4326)
                centroid = JSONDeserializer().deserialize(geom.centroid.json)
                geom_list.append(centroid['coordinates'])
            arr = np.asarray(geom_list)
            length = arr.shape[0]
            sum_x = np.sum(arr[:, 0])
            sum_y = np.sum(arr[:, 1])
            wkt = 'POINT(%s, %s)' % (sum_x/length, sum_y/length)
            return GEOSGeometry(wkt, srid = 4326).json
            
    else:
        return None        
