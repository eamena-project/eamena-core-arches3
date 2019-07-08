import uuid
import json

class NewResource():

    def __init__(self, entity, node_lookup={}, label_lookup={}, period_lookup={},
            label_transformations={}, assessor_lookup={}):

        self.data = entity
        self.resid = entity['entityid']
        
        if entity['entitytypeid'] == "HERITAGE_RESOURCE_GROUP.E27":
            self.restype = "HERITAGE_PLACE.E27"
        else:
            self.restype = entity['entitytypeid']

        self.node_lookup = node_lookup
        self.label_lookup = label_lookup
        self.period_lookup = period_lookup
        self.label_transformations = label_transformations
        self.assessor_lookup = assessor_lookup
        self.groups = 0
        self.rows = False
        self.errors = []
        self.missing_labels = []
        self.has_extended_dates = False
        self.has_date_assessed = False
        self.v1_nodes = []
        self.msgs = []

    def advance_group(self):

        self.groups += 1

    def print_messages(self):
        
        for msg in self.msgs:
            print msg

    def convert_concept_label(self, label, entitytype):
        
        try:
            label_options = self.label_lookup[entitytype]
        except KeyError:
            self.errors.append("WARNING - invalid node name: {}".format(entitytype))
            return ""

        # check to see if this is one of the labels that needs to be
        # transformed and do so if necessary
        v1_label = label.lower().lstrip()
        if entitytype in self.label_transformations:
            if v1_label in self.label_transformations[entitytype]:
                v1_label = self.label_transformations[entitytype][v1_label]

        try:
            value = label_options[v1_label]
        except KeyError:
            self.missing_labels.append((entitytype,label))
            msg = "WARNING - invalid label in {} - '{}'".format(entitytype, v1_label)
            self.errors.append(msg)
            temp = label_options.values()[0]

            return temp
        return value

    def get_value_from_entity(self, entity, v2node_name=None):
    
        # if a v2 node name hasn't been passed in (which should be common)
        # then use the node name lookup
        if v2node_name is None:
            try:
                v2node_name = self.node_lookup[entity['entitytypeid']]
            except:
                msg = "WARNING - no v2 node to match this entity:\n{}".format(str(entity))
                self.errors.append(msg)

        # initial creation of return object
        ret = {"v2node":v2node_name,"value":None}

        # In this one case, the v1 node is a domains node SOMETIMES, but in v2 it
        # isn't. So disregard the business table name and return the label
        entitytype = entity['entitytypeid']
        if entitytype == "ASSESSOR_NAME_TYPE.E55":
            
            value = entity['label']
            # if this is one where the uuid was also in the label field,
            # then use the lookup table to get the real name.
            if value in self.assessor_lookup:
                value = self.assessor_lookup[value]

            ret['value'] = value
            return ret

        elif entitytype == "VALUE_OF_MEASUREMENT.E60":
            value = entity['value'].replace(",","")
            if len(value.split(".")) > 2:
                value = value.replace(".","",1)

            # strip out any letters
            okchars = [str(i) for i in range(10)] + ["."]
            value = "".join([i for i in value if i in okchars])

            ret['value'] = value
            return ret

        # Otherwise, process the return value based on business table name.
        datatype = entity['businesstablename']
        if datatype == "dates":
            value = entity['value'][:10]
        elif datatype == "domains":
            label = entity['label']
            if not v2node_name:
                v2node_name = self.node_lookup[entitytype]
            
            # somehow the following values got stored in v1. changing
            # them to a default value of Unknown, which is a valid label for
            # both FEATURE_NUMBER_TYPE.E55 and INTERPRETATION_NUMBER_TYPE.E55
            if (v2node_name == "FEATURE_NUMBER_TYPE.E55" and label in ["Toponym", "Negligible"])\
                or (v2node_name == "INTERPRETATION_NUMBER_TYPE.E55" and label == "High"):
                repl = "Unknown"
                msg = "WARNING - invalid label in {} - '{}' - replacing with '{}'".format(
                    v2node_name, label, repl)
                self.errors.append(msg)
                ret['value'] = repl
                return ret

            value = self.convert_concept_label(label,v2node_name)
        elif datatype == "geometries":
            value = entity['value']
        else:
            value = entity['value']

        ret['value'] = value
        return ret

    def handle_one_nested(self, branch_entity):
        """Pattern one is a two node branch, each with a business value.
        Most typical example:
        NAME.E41
           \
          NAME_TYPE.E42
        Pass the name entity to this function and rows will be made for NAME
        and NAME TYPE."""

        self.make_row_from_entity(branch_entity, advance_group=False)

        if len(branch_entity['child_entities']) == 0:
            if branch_entity['entitytypeid'] == "NAME.E41":
                default = "Toponym"
                branch_entity['child_entities'] = [{
                    "label":default,
                    "businesstablename":"domains",
                    "entitytypeid":"NAME_TYPE.E55"
                }]
                self.errors.append("WARNING - expected NAME_TYPE.E55 but found none."
                    " '{}' used as default.".format(default))

            elif branch_entity['entitytypeid'] == "SITE_FUNCTION_TYPE.E55":
                self.errors.append("WARNING - expected SITE_FUNCTION_CERTAINTY_TYPE.E55 but found none."
                    " Leaving field blank.")
                return

            else:
                self.errors.append("WARNING - expected child entity in this branch but found"\
                    " none: {}".format(str(branch_entity)))
                return
        
        self.make_row_from_entity(branch_entity['child_entities'][0])

    def handle_condition_assessment_branch(self, branch_entity):

        # iterate the child entities of the branch
        for subbranch in branch_entity['child_entities']:
            if subbranch['entitytypeid'] == "ASSESSMENT_TYPE.E55":
                self.make_row_from_entity(subbranch, advance_group=False)
                if len(subbranch['child_entities']) == 1:
                    self.handle_one_nested(subbranch['child_entities'][0])
            elif subbranch['entitytypeid'] == "CONDITION_STATE.E3":
                for ssb in subbranch['child_entities']:
                    if ssb['entitytypeid'] == "THREAT_STATE.E3":
                        # this will iterate the two nodes that are always
                        # child_entities of THREAT_STATE.E3: THREAT_CAUSE_STATE.E3
                        # and THREAT_TYPE_STATE.E3, and add the children of those
                        # entities as rows.
                        for parent in ssb['child_entities']:
                            for child in parent['child_entities']:
                                if len(child['child_entities']) > 0:
                                    self.errors.append("WARNING - unexpected child entity: {}".format(str(child)))
                                self.make_row_from_entity(child, advance_group=False)
                        self.advance_group()
                    elif ssb['entitytypeid'] == "DISTURBANCE_STATE.E3":
                        # first deal with the date situation by iterating to find
                        # all the dates, and then using their presence/absence to
                        # determine which v2 nodes they should be placed in.
                        sdate, edate = None, None
                        for sssb in ssb['child_entities']:
                            if sssb['entitytypeid'] == "DISTURBANCE_DATE_START.E49":
                                sdate = sssb
                            if sssb['entitytypeid'] == "DISTURBANCE_DATE_END.E49":
                                edate = sssb

                        # now the logic to set the dates into new node names
                        if sdate and edate:
                            self.make_row_from_entity(sdate, advance_group=False,
                                v2node="DISTURBANCE_DATE_FROM.E61")
                            self.make_row_from_entity(edate, advance_group=False,
                                v2node="DISTURBANCE_DATE_TO.E61")
                        elif sdate:
                            self.make_row_from_entity(sdate, advance_group=False,
                                v2node="DISTURBANCE_DATE_OCCURRED_ON.E61")
                        elif edate:
                            self.make_row_from_entity(edate, advance_group=False,
                                v2node="DISTURBANCE_DATE_OCCURRED_BEFORE.E61")

                        # reiterate all child entities to deal with the non-date branches
                        for sssb in ssb['child_entities']:
                            if sssb['entitytypeid'].startswith("DISTURBANCE_DATE"):
                                continue
                            if sssb['entitytypeid'] == "DISTURBANCE_CAUSE_STATE.E3":
                                for e in sssb['child_entities']:
                                    self.make_row_from_entity(e, advance_group=False)
                            elif sssb['entitytypeid'] == "DISTURBANCE_TYPE.E55":
                                self.make_row_from_entity(sssb, advance_group=False)
                                
                            # Handle the multiple sets of Effects and Effect Certainties. This
                            # relies on these nodes to have been added to the installation ahead
                            # of this conversion process.
                            elif sssb['entitytypeid'] == "DISTURBANCE_EFFECT_STATE.E3":
                                numbered_effects = {}
                                for i in sssb['child_entities']:

                                    nodename = i['entitytypeid']
                                    try:
                                        effect_num = int(nodename.split("_")[2])
                                    except Exception as e:
                                        effect_num = 0
                                    
                                    if effect_num in numbered_effects:
                                        numbered_effects[effect_num].append(i)
                                    else:
                                        numbered_effects[effect_num] = [i]

                                effects_keys = numbered_effects.keys()
                                effects_keys.sort()
                                for index, k in enumerate(effects_keys):
                                    usenum = index + 1
                                    for i in numbered_effects[k]:
                                        nodename = i['entitytypeid']
                                        if "CERTAINTY" in nodename:
                                            self.make_row_from_entity(i,advance_group=False,
                                                v2node="EFFECT_CERTAINTY_{}.I6".format(usenum))

                                        else:
                                            self.make_row_from_entity(i,advance_group=False,
                                                v2node="EFFECT_TYPE_{}.I4".format(usenum))

                            else:
                                msg = "WARNING - branch not accounted for DISTURBANCE_STATE.E3 > {}".format(
                                    sssb['entitytypeid'])
                                self.errors.append(msg)

                        self.advance_group()
                    elif ssb['entitytypeid'] in ["CONDITION_TYPE.E55", "DISTURBANCE_EXTENT_TYPE.E55"]:
                        self.msgs.append(ssb['entitytypeid'])
                        self.make_row_from_entity(ssb)
                    elif ssb['entitytypeid'] == "DATE_CONDITION_ASSESSED.E49":
                        self.has_date_assessed = True
                    else:
                        msg = "WARNING - branch not accounted for CONDITION_STATE.E3 > {}".format(
                            ssb['entitytypeid'])
                        self.errors.append(msg)

    def make_row_from_entity(self, entity, advance_group=True, v2node=None):

        result = self.get_value_from_entity(entity, v2node_name=v2node)

        row = [self.resid, self.restype, result['v2node'], result['value'], self.groups]
        self.rows.append(row)

        if advance_group is True:
            self.advance_group()

    def make_rows(self):
        # print "\n"+(20*"$")
        self.rows = list()
        top_branches = self.data['child_entities']
        sft_ct, fea_ct = 0, 0
        used_tb = list()

        for tb_entity in top_branches:
            
            entitytype = tb_entity['entitytypeid']
            # print entitytype
            if len(tb_entity['child_entities']) == 0 and tb_entity['businesstablename'] != "":
                # the site_id node is converted to a NAME.E41, and Designation is set as the
                # NAME_TYPE.E55 node
                if entitytype == "SITE_ID.E42":
                    self.make_row_from_entity(tb_entity, advance_group=False)
                    nametype_entity = {
                        "label":"Designation",
                        "businesstablename":"domains",
                        "entitytypeid":"NAME_TYPE.E55"
                    }
                    self.make_row_from_entity(nametype_entity)
                else:
                    
                    self.make_row_from_entity(tb_entity)
                used_tb.append(tb_entity)

        for tb_entity in top_branches:
            entitytype = tb_entity['entitytypeid']
            if entitytype in ["NAME.E41", "DESCRIPTION.E62"]:
                self.handle_one_nested(tb_entity)
                used_tb.append(tb_entity)

            ## Place site function into its own branch. This relies on extra
            ## nodes to have been added to the resource graph.
            if entitytype == "SITE_FUNCTION_TYPE.E55":
                self.handle_one_nested(tb_entity)
                used_tb.append(tb_entity)

        ## nodes in the place branches are stored separately but should be
        ## combined, as far as I can tell. Thus the group number is not advanced
        ## until this entire iteration is complete.
        for tb_entity in top_branches:
            entitytype = tb_entity['entitytypeid']
            if entitytype == "PLACE.E53":
                for e in tb_entity['child_entities']:
                    if e['entitytypeid'] == "PLACE_SITE_LOCATION.E53":
                        # this entity occurs only 3 times in the v1 dataset. it contains the
                        # DESCRIPTION_OF_LOCATION.E62 node, whose value in two cases is "<br>"
                        # and in the third case is "cairn fields." Also, there is no analogous
                        # node in the v2 graphs. For these reasons, I'm not going to migrate this
                        # entity.
                        continue
                    else:
                        self.make_row_from_entity(e, advance_group=False)
                        for little_e in e['child_entities']:
                            if little_e['entitytypeid'] == "GEOMETRY_QUALIFIER.E55":
                                # this entity occurs in only 2 resources, and there
                                # is nowhere to put in the v2 graph, so it will be
                                # ignored.
                                continue
                            self.make_row_from_entity(little_e, advance_group=False)
                used_tb.append(tb_entity)
        self.advance_group()

        for tb_entity in top_branches:
            entitytype = tb_entity['entitytypeid']

            if entitytype == "CONDITION_ASSESSMENT.E14":
                used_tb.append(tb_entity)
                self.handle_condition_assessment_branch(tb_entity)

            if entitytype == "RIGHT.E30":
                used_tb.append(tb_entity)
                for ce in tb_entity['child_entities']:
                    for e in ce['child_entities']:
                        if e['entitytypeid'] == "TYPE_OF_DESIGNATION_OR_PROTECTION.E55":
                            self.make_row_from_entity(e, advance_group=False)
                        elif e['entitytypeid'] == "TIME_SPAN_OF_DESIGNATION_OR_PROTECTION.E52":
                            for dentity in e['child_entities']:
                                self.make_row_from_entity(dentity, advance_group=False)
                    self.advance_group()

            if entitytype == "PRODUCTION.E12":

                used_tb.append(tb_entity)
                for csb in tb_entity['child_entities']:
                    for sb in csb['child_entities']:
                        if sb['entitytypeid'] == "CULTURAL_PERIOD.E55":
                            if sb['label'] in self.period_lookup:
                                cp = self.period_lookup[sb['label']]
                            else:
                                self.missing_labels.append((sb['entitytypeid'],sb['label']))
                                print "missing period lookup:", sb['label']
                                continue
                            sb['label'] = cp['cp']
                            self.make_row_from_entity(sb, advance_group=False)

                            if cp['sp'] != "":
                                mock_entity = {
                                    "label":cp['sp'],
                                    "businesstablename":"domains",
                                    "entitytypeid":None
                                }
                                self.make_row_from_entity(mock_entity, v2node="CULTURAL_PERIOD_DETAIL_TYPE.E55",
                                    advance_group=False)

                            if len(sb['child_entities']) == 1:
                                self.make_row_from_entity(sb['child_entities'][0], advance_group=False)
                            else:
                                print self.resid, len(sb['child_entities'])

                            self.advance_group()

                        elif sb['entitytypeid'] == "FEATURE_EVIDENCE_ASSIGNMENT.E17":
                            for fea in sb['child_entities']:
                                self.make_row_from_entity(fea, advance_group=False)
                            self.advance_group()

                        ## this is where the SITE_FUNCTION_TYPE and SITE_FUNCTION_CERTAINTY
                        ## entities are pushed off to a completely separate branch.
                        elif sb['entitytypeid'] == "FEATURE_EVIDENCE_INTERPRETATION_ASSIGNMENT.E17":
                            for fei in sb['child_entities']:
                                self.make_row_from_entity(fei, advance_group=False)
                            self.advance_group()

                        elif sb['entitytypeid'] == "TIME-SPAN_PHASE.E52":
                            if len(sb['child_entities']) > 0:
                                self.has_extended_dates = True

                        else:
                            msg = "WARNING - branch not accounted for PHASE_TYPE_ASSIGNMENT.E17 > {}".format(
                                sb['entitytypeid'])
                            self.errors.append(msg)

            if entitytype == "MEASUREMENT_TYPE.E55":
                used_tb.append(tb_entity)
                self.make_row_from_entity(tb_entity, advance_group=False)
                self.handle_one_nested(tb_entity['child_entities'][0])

        top_branches = [i for i in top_branches if not i in used_tb]
        if len(top_branches) > 0:
            print "UNHANDLED TOP BRANCHES:", [i['entitytypeid'] for i in top_branches]

    def flatten_entity(self, entity=None):

        if entity is None:
            entity = self.data
        if entity['businesstablename'] != "":
            
            value = entity.get('value',None)
            self.v1_nodes.append(
                (entity['entitytypeid'],value,entity['label'])
            )
        if not 'child_entities' in entity:
            entity['child_entities'] = []
        for child in entity['child_entities']:
            node_list = self.flatten_entity(child)

    def test_business_data_ct(self):

        self.flatten_entity()

        v1_node_names = [i[0] for i in self.v1_nodes]
        v2_node_names = [i[2] for i in self.rows]

        reported = False
        for v1_node in sorted(set(v1_node_names)):
            if v1_node.startswith("DISTURBANCE_EFFECT_"):
                continue
            if v1_node == "FROM_DATE.E55" or v1_node == "TO_DATE.E55":
                continue
            if v1_node == "DATE_CONDITION_ASSESSED.E49":
                # with open(r"idlists\E27s_with_DATE_CONDITION_ASSESSED.E49.txt", "a") as f:
                    # f.write(self.resid+"\n")
                continue
            if v1_node == "GEOMETRY_QUALIFIER.E55":
                # with open(r"idlists\E27s_with_GEOMETERY_QUALIFIER.E55.txt", "a") as f:
                    # f.write(self.resid+"\n")
                continue
            if v1_node == "DESCRIPTION_OF_LOCATION.E62":
                # with open(r"idlists\E27s_with_DESCRIPTION_OF_LOCATION.E62.txt", "a") as f:
                    # f.write(self.resid+"\n")
                continue
                
            if v1_node.startswith("DISTURBANCE_DATE_"):
                # with open(r"idlists\E27s_with_DESCRIPTION_OF_LOCATION.E62.txt", "a") as f:
                    # f.write(self.resid+"\n")
                continue

            v1_occurrences = v1_node_names.count(v1_node)

            v2_node = self.node_lookup[v1_node]
            v2_occurrences = v2_node_names.count(v2_node)

            if v1_occurrences != v2_occurrences:

                if v1_node in ["NAME.E41", "NAME_TYPE.E55", "SITE_ID.E42"]:
                    if v1_occurrences < v2_occurrences:
                        continue
                if v1_node == "SITE_FUNCTION_TYPE.E55":
                    if v1_occurrences < v2_occurrences:
                        # this is ok because two separate branches from v1
                        # are combined into one, so there are often more v2
                        # occurrences than v1 occurrences.
                        continue

                print v1_node, v1_occurrences, v2_node, v2_occurrences
