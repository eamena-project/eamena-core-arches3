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


from django.views.decorators.csrf import csrf_exempt
from arches.app.utils.betterJSONSerializer import JSONSerializer, JSONDeserializer
from arches.app.utils.JSONResponse import JSONResponse
from arches.app.search.elasticsearch_dsl_builder import Bool, Query, Nested, Terms, GeoShape
from arches.app.search.search_engine_factory import SearchEngineFactory
from django.contrib.gis.geos import GEOSGeometry
from arches.app.models.resource import Resource
from arches.app.models.entity import Entity

import numpy as np


@csrf_exempt
def create_resources(request):
	if request.method == 'POST':
		json_resources = JSONDeserializer().deserialize(request.body)
		info_res_list = []
		info_res = {
			'image_url': '',
			'geometry': '',
			'capture_date': '',
			'caption': '',
			'assessor_name': '',
			'related_to': []
			
		}
# 			resource = Resource({'entitytypeid': 'INFORMATION_RESOURCE.E73'})
# 			schema = Entity.get_mapping_schema('INFORMATION_RESOURCE.E73')
# 	return JSONResponse({'success': True})

@csrf_exempt
def return_resources(request):
	'''Endpoint to return nearest 1000 EAMENA resources given a GeoJSON bounding box'''
	if request.method == 'POST':
		json_collection = []
		json_res = {
			'resource_type': '',
			'resource_id': '',
			'resource_name' : '',
			'centroid': ''
		}
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