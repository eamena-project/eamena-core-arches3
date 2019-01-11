define(['jquery', 
    'summernote',
    'underscore', 
    'knockout-mapping', 
    'views/forms/base', 
    'views/forms/sections/branch-list',
    'views/forms/sections/validation',
    'bootstrap-datetimepicker',
    ], 
    function ($, summernote, _, koMapping, BaseForm, BranchList, ValidationTools) {
        var vt = new ValidationTools;
        return BaseForm.extend({
            initialize: function() {
                
                BaseForm.prototype.initialize.apply(this);                
                var self = this;
                var date_picker = $('.datetimepicker').datetimepicker({pickTime: false});
                date_picker.on('dp.change', function(evt){
                    $(this).find('input').trigger('change'); 
                });

                this.addBranchList(new BranchList({
                    el: this.$el.find('#investigator-name-section')[0],
                    data: this.data,
                    dataKey: 'INVESTIGATION_ACTIVITY.E7',
                    validateBranch: function (nodes) {
                        var ck1 = vt.nodesHaveValues(nodes, {"canBeEmpty":['INVESTIGATOR_ROLE_TYPE.E55']})
                        var ck2 = vt.isValidDate(nodes,'DATE_OF_ACTIVITY.E48');
                        return ck1 && ck2;
                    }
                }));
            }
        });
    }
);