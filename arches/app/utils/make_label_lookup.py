from arches.app.models.models import EntityTypes, Concepts, Values, ConceptRelations
import json

def collect_concepts(node_conceptid, full_concept_list = []):
    ''' Collects a full list of child concepts given the conceptid of the node. Returns a list of a set of concepts, i.e. expounding the duplicates'''
    concepts_in_node = ConceptRelations.objects.filter(conceptidfrom = node_conceptid)
    if concepts_in_node.count() > 0:
        full_concept_list.append(node_conceptid) 
        for concept_in_node in concepts_in_node:
            
            collect_concepts(concept_in_node.conceptidto_id, full_concept_list)
    else:
        full_concept_list.append(node_conceptid)
    return list(set(full_concept_list))

def generate_lookup(outfile):
    outlookup = dict()
    a = EntityTypes.objects.filter(businesstablename="domains")
    for c in a:
        all_concepts = collect_concepts(c.conceptid_id,full_concept_list=[])
        # print len(all_concepts)
        lookup = {}
        for v in all_concepts:
            cobj = Concepts.objects.get(pk=v)
            labels = Values.objects.filter(conceptid_id=v,valuetype_id="prefLabel")
            for label in labels:
                lookup[label.value.lower()] = cobj.legacyoid
        outlookup[c.pk] = lookup
    print "now writing..."
    with open(outfile,"wb") as outjson:
        json.dump(outlookup,outjson,indent=1)
    print "    done"