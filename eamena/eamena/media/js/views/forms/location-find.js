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
                    el: this.$el.find('#location-section')[0],
                    data: this.data,
                    dataKey: 'PLACE.E53',
                    rules: true,
                    validateBranch: function (nodes) {
                        var ck0 = this.validateHasValues(nodes);
                        return ck0;
                    }
                }));
            }
        });
    }
);