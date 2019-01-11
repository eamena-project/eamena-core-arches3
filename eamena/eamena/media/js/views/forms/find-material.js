define(['jquery', 
    'underscore', 
    'knockout-mapping', 
    'views/forms/base', 
    'views/forms/sections/branch-list',
    'views/forms/sections/validation',], 
    function ($, _, koMapping, BaseForm, BranchList, ValidationTools) {
        var vt = new ValidationTools;
        return BaseForm.extend({
            initialize: function() {
                BaseForm.prototype.initialize.apply(this);

                this.addBranchList(new BranchList({
                    el: this.$el.find('#material-section')[0],
                    data: this.data,
                    dataKey: 'PRODUCTION.E12',
                    rules: true,
                    validateBranch: function (nodes) {
                        var ck0 = vt.nodesHaveValues(nodes,{"mustBeFilled":["MATERIAL.E57"]});
                        return ck0;
                    }
                }));
            }
        });
    }
);