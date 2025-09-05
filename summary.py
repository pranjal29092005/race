import os, sys, json


class AnalysisSummary:
    def __init__(self):
        super().__init__()
        self.portfolio = True
        self.reins_measures = False
        self.defaults = {
            'portfolio': ['TIV', 'ContractLoss_Value All Types_EL', 'ContractLoss_Value All Types_GL', 'GroundUpLoss'],
            'sov': ['TIV', 'GroundUpLoss'],
            'reins':
                ['Net Pre Cat Exposed Limit', 'Net of Fac Exposed Limit', 'Fac Exposed Limit', 'Treaty Exposed Limit']
        }
        self.script = '''
string errorMessage;
array<uint> indices;
indices.insertLast(0);
if (!GetSummaryV2('Summary', indices, errorMessage)) {
        SendErrorWithCode(errorMessage);
}
        '''

    def to_json(self):
        measures = set()
        if self.portfolio:
            [measures.add(m) for m in self.defaults['portfolio']]
        else:
            [measures.add(m) for m in self.defaults['sov']]

        if self.portfolio and self.reins_measures:
            [measures.add(m) for m in self.defaults['reins']]
        measures = list(measures)
        obj = {'Summary': {'AdditionalMeasures': measures}}
        obj['Script'] = self.script.replace('\n', ' ')
        return obj
