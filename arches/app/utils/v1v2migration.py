
class NewResource():
    
    def __init__(self, entity, node_lookup={}, label_lookup={}, period_lookup={}, label_transformations={}):

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
        self.groups = 0
        self.rows = False
        self.errors = []
        self.missing_labels = []
    
    def advance_group(self):

        self.groups += 1

    def convert_concept_label(self, label, entitytype):
        
        try:
            label_options = self.label_lookup[entitytype]
        except KeyError:
            self.errors.append("invalid node name: {}".format(entitytype))
            return ""
            
        label_cln = label.lower().lstrip().rstrip()
        # check to see if this is one of the labels that needs to be
        # transformed and do so if necessary
        v1_label = label_cln
        if entitytype in self.label_transformations:

            if label_cln in self.label_transformations[entitytype]:
                v1_label = self.label_transformations[entitytype][label_cln]

        try:
            value = label_options[v1_label]
        except KeyError:
            self.missing_labels.append((entitytype,label))
            self.errors.append("WARNING: invalid label in "+entitytype+" - "+ v1_label)
            temp = label_options.values()[0]

            return temp
        return value

    def get_value_from_entity(self, entity, v2node_name=None):

        datatype = entity['businesstablename']
        if datatype == "dates":
            value = entity['value'][:10]
        elif datatype == "domains":
            # In this one case, the v1 node is a domains node, but in v2 items
            # isn't. So use the label, instead of the value.
            if entity['entitytypeid'] == "ASSESSOR_NAME_TYPE.E55":
                value = entity['label']
            else:
                if not v2node_name:
                    v2node_name = self.node_lookup[entity['entitytypeid']]
                value = self.convert_concept_label(entity['label'],v2node_name)
        elif datatype == "geometries":
            value = entity['value']
        else:
            value = entity['value']

        return value

    def handle_one_nested(self, branch_entity):
        """Pattern one is a two node branch, each with a business value.
        Most typical example:
        NAME.E41
           \
          NAME_TYPE.E42
        Pass the name entity to this function and rows will be made for NAME
        and NAME TYPE."""

        if len(branch_entity['child_entities']) == 0 and branch_entity['entitytypeid'] == "NAME.E41":
            branch_entitymock_entity = {
                    "label":"Toponym",
                    "businesstablename":"domains",
                    "entitytypeid":"NAME_TYPE.E55"
                }
            self.errors.append("WARNING: expected NAME_TYPE.E55 but found none."
                "'Toponym' used as default.")

        self.make_row_from_entity(branch_entity, advance_group=False)
        try:
            self.make_row_from_entity(branch_entity['child_entities'][0])
        except IndexError:
            self.errors.append("WARNING: expected child entity in this branch but found none:")
            self.errors.append(str(branch_entity))

    def handle_condition_assessment_branch(self, branch_entity):
        
        # there's always one child entity
        subbranch = branch_entity['child_entities'][0]
        if subbranch['entitytypeid'] == "ASSESSMENT_TYPE.E55":
            self.make_row_from_entity(subbranch, advance_group=False)
            self.handle_one_nested(subbranch['child_entities'][0])
        elif subbranch['entitytypeid'] == "CONDITION_STATE.E3":
            ssb = subbranch['child_entities'][0]
            if ssb['entitytypeid'] == "THREAT_STATE.E3":
                # this will iterate the two nodes that are always
                # child_entities of THREAT_STATE.E3: THREAT_CAUSE_STATE.E3
                # and THREAT_TYPE_STATE.E3, and add the children of those
                # entities as rows.
                for parent in ssb['child_entities']:
                    for child in parent['child_entities']:
                        if len(child['child_entities']) > 0:
                            self.errors.append("WARNING - unexpected child entity:")
                            self.errors.append(str(child))
                        self.make_row_from_entity(child, advance_group=False)
                self.advance_group()
            elif ssb['entitytypeid'] == "DISTURBANCE_STATE.E3":
                # print "  DISTURBANCE_STATE.E3"

                # first deal with the date situationn by iterating to find
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
                        for i in sssb['child_entities']:

                            nodename = i['entitytypeid']
                            effect_num = int(nodename.split("_")[2])

                            if "CERTAINTY" in nodename:
                                self.make_row_from_entity(i,advance_group=False,
                                    v2node="EFFECT_CERTAINTY_{}.I6".format(effect_num))

                            else:
                                self.make_row_from_entity(i,advance_group=False,
                                    v2node="EFFECT_TYPE_{}.I4".format(effect_num))

                    else:
                        self.errors.append("WARNING - this branch not accounted for {}".format(sssb['entitytypeid']))

                self.advance_group()
            elif ssb['entitytypeid'] in ["CONDITION_TYPE.E55", "DISTURBANCE_EXTENT_TYPE.E55"]:
                self.make_row_from_entity(ssb)
            else:
                print "WARNING - this branch not accounted for", ssb['entitytypeid']

    def make_row_from_entity(self, entity, advance_group=True, v2node=None):
    
        # if a v2 node name hasn't been passed in (which should be common)
        # then use the node name lookup
        if v2node is None:
            v2node = self.node_lookup[entity['entitytypeid']]
            
        value = self.get_value_from_entity(entity, v2node)

        row = [self.resid, self.restype, v2node, value, self.groups]
        self.rows.append(row)
        
        if advance_group is True:
            self.advance_group()

    def make_rows(self):

        self.rows = list()
        top_branches = self.data['child_entities']
        sft_ct, fea_ct = 0, 0
        used_tb = list()
        for tb_entity in top_branches:

            entitytype = tb_entity['entitytypeid']
            if len(tb_entity['child_entities']) == 0 and tb_entity['businesstablename'] != "":

                self.make_row_from_entity(tb_entity)
                used_tb.append(tb_entity)
        
        
        for tb_entity in top_branches:
            entitytype = tb_entity['entitytypeid']
            if entitytype in ["NAME.E41", "DESCRIPTION.E62"]:
                self.handle_one_nested(tb_entity)
                used_tb.append(tb_entity)

            ## Place site function into it's own branch. This relies on extra
            ## nodes to have been added to the resource graph.
            if entitytype == "SITE_FUNCTION_TYPE.E55":
                self.make_row_from_entity(tb_entity, advance_group=False)
                self.make_row_from_entity(tb_entity['child_entities'][0])
                sft_ct += 1

        ## nodes in the place branches are stored separately but should be
        ## combined, as far as I can tell. Thus the group number is not advanced
        ## until this entire iteration is complete.
        geom, certainty = None, None
        for tb_entity in top_branches:
            entitytype = tb_entity['entitytypeid']
            if entitytype == "PLACE.E53":
                for e in tb_entity['child_entities']:
                    if e['entitytypeid'] == "SITE_LOCATION_CERTAINTY_TYPE.E55":
                        certainty = e
                    elif e['entitytypeid'] == "SPATIAL_COORDINATES_GEOMETRY.E47":
                        geom = e
                    else:
                        self.make_row_from_entity(e, advance_group=False)
                        if len(e['child_entities']) > 0:
                            for little_e in e['child_entities']:
                                self.make_row_from_entity(little_e, advance_group=False)
                used_tb.append(tb_entity)
        ## it's necessary to have collected these items ahead of time 
        ## so that they are only added if both are present.
        if geom is not None and certainty is not None:
            self.make_row_from_entity(geom, advance_group=False)
            self.make_row_from_entity(certainty, advance_group=False)
        self.advance_group()

        for tb_entity in top_branches:
            entitytype = tb_entity['entitytypeid']

            if entitytype == "CONDITION_ASSESSMENT.E14":
                used_tb.append(tb_entity)
                self.handle_condition_assessment_branch(tb_entity)
            
            if entitytype == "RIGHT.E30":
                used_tb.append(tb_entity)
                for e in tb_entity['child_entities'][0]['child_entities']:
                    if e['entitytypeid'] == "TYPE_OF_DESIGNATION_OR_PROTECTION.E55":
                        self.make_row_from_entity(e, advance_group=False)
                    elif e['entitytypeid'] == "TIME_SPAN_OF_DESIGNATION_OR_PROTECTION.E52":
                        for dentity in e['child_entities']:
                            self.make_row_from_entity(dentity, advance_group=False)
                self.advance_group()
            
            if entitytype == "PRODUCTION.E12":

                used_tb.append(tb_entity)
                for sb in tb_entity['child_entities'][0]['child_entities']:

                    if sb['entitytypeid'] == "CULTURAL_PERIOD.E55":
                        cp = self.period_lookup[sb['label']]
                        sb['label'] = cp['cp']
                        self.make_row_from_entity(sb, advance_group=False)

                        if cp['sp'] != "":
                            mock_entity = {
                                "label":cp['sp'],
                                "businesstablename":"domains",
                                "entitytypeid":None
                            }
                            self.make_row_from_entity(mock_entity, v2node="CULTURAL_PERIOD_DETAIL_TYPE.E55")
                        else:
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

                    else:
                        self.errors.append("WARNING - this branch not accounted for {}".format(sb['entitytypeid']))

        top_branches = [i for i in top_branches if not i in used_tb]
