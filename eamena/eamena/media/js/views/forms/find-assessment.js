define(['jquery', 
    'summernote', 
    'underscore', 
    'knockout-mapping', 
    'views/forms/base', 
    'views/forms/sections/branch-list',
    'views/forms/sections/validation',], 
    function ($, summernote, _, koMapping, BaseForm, BranchList, ValidationTools) {
        var vt = new ValidationTools;
        return BaseForm.extend({
            initialize: function() {
                BaseForm.prototype.initialize.apply(this);

                this.addBranchList(new BranchList({
                    el: this.$el.find('#id-section')[0],
                    data: this.data,
                    dataKey: 'FIND_ID.E42',
                    validateBranch: function (nodes) {
                        var ck0 = vt.nodesHaveValues(nodes);
                        return ck0;
                    }
                }));

                this.addBranchList(new BranchList({
                    el: this.$el.find('#material-section')[0],
                    data: this.data,
                    dataKey: 'PRODUCTION.E12',
                    validateBranch: function (nodes) {
                        var ck0 = vt.nodesHaveValues(nodes,{"canBeEmpty":[
                            "FIND_SUB_CATEGORY_TYPE.E55",
                            "FIND_DETAILED_CATEGORY_TYPE.E55",
                            "TECHNIQUE_TYPE.E55",
                        ]});
                        return ck0;
                    }
                }));
                
                this.addBranchList(new BranchList({
                    el: this.$el.find('#notes-section')[0],
                    data: this.data,
                    dataKey: 'FIND_NOTES.E62',
                    validateBranch: function (nodes) {
                        var ck0 = vt.nodesHaveValues(nodes);
                        return ck0;
                    }
                }));
            }
        });
    }
);