import os, sys, json, click, uuid
import er_aws, er_db, er_utils
from icecream import ic
import pandas as pd

peril_table = None
measure_map = {
    'tiv': 'TIV',
    'gul': 'GroundUpLoss',
    'gel': 'ContractLoss_Value All Types_EL',
    'gl': 'ContractLoss_Value All Types_GL'
}


def get_perils(env):
    global peril_table
    if peril_table is not None:
        return peril_table
    with er_db.get_db_conn(env) as conn:
        query = '''
select ph.peril_id, ph.sub_peril_id, ph.peril_name, ph.sub_peril_name, pp.peril_code peril_code, sp.peril_code sub_peril_code
FROM
(select peril_id,peril_name,sub_peril_id,sub_peril_name from "GetPerilHierarchy"()) ph
INNER JOIN
(select peril_id,peril_name, peril_code from "GetSubPerilData"()) sp
ON
ph.sub_peril_id = sp.peril_id
INNER JOIN
(select peril_id, peril_name, peril_code from "GetPerilData"()) pp
ON
ph.peril_id = pp.peril_id
        '''
        res = er_db.exec_stmt(conn, query, fetch_all=True)
        peril_table = pd.DataFrame(res)


def print_arg(label, value):
    click.secho(f'{label}: ', fg='red', nl=False, bold=True)
    click.secho(value)


def validate_peril(ctx, param, value):
    global peril_table
    if value.lower().endswith('xx'):
        raise click.BadParameter('Shape spider cannot work with perils. A sub peril code is expected')
    get_perils(ctx.params['env'])
    sp_code = value.upper()
    return sp_code


def validate_measure(ctx, param, value):
    global measure_map
    if ctx.params['portfolio'] is False and value in ['gel', 'gl']:
        raise click.BadParameter('Spider for sov is not supported for Gross Exposed Limit and Gross Loss')
    return measure_map[value]


def get_script_field():
    return {
        'Script':
            '''
    string errorMessage;
    uint index = 0;
    if (!AsyncSpiderAnalysis('AsyncSpiderAnalysis', errorMessage)) {
        SendErrorWithCode(errorMessage);
    }
    '''
    }


def get_spider_settings(exp_id, portfolio, peril, measure):
    global peril_table
    sub_table = peril_table[peril_table.sub_peril_code == peril]
    addlMeasures = ['TIV', '#Assets', 'GroundUpLoss']
    if portfolio:
        addlMeasures.extend(['ContractLoss_Value All Types_EL', 'ContractLoss_Value All Types_GL', '#Contracts'])
    return {
        'AsyncSpiderAnalysis':
            {
                'AdditionalMeasure': addlMeasures,
                'Exposure Id': exp_id,
                'Count': 20,
                'CurrencyCode': 'USD',
                'DamageFactors': [1],
                'eventRadii': [400],
                'IncludeAllContracts': True,
                'ExposureFilterSets':
                    {
                        'AssetModel':
                            'ERBASICS',
                        'FilterList':
                            [
                                {
                                    'AndOr':
                                        'AND',
                                    'FilterList':
                                        [
                                            {
                                                'AndOr': 'AND',
                                                'AssetType': 'Site',
                                                'Attribute': 'Cause Of Loss',
                                                'Operator': 'EQ',
                                                'Value': sub_table['sub_peril_code'].iloc[0]
                                            }
                                        ]
                                }
                            ]
                    },
                'damageAdjustment': 1,
                'deltaOffset': 800,
                'threshold': 0,
                'Results Key': str(uuid.uuid1()),
                'SpiderAnalysisBy': measure,
                'ObjectSubType': 'portfolio' if portfolio else 'sov',
                'xRes': 2048,
                'yRes': 1290,
                'peril': sub_table['peril_name'].iloc[0],
                'subPeril': sub_table['sub_peril_name'].iloc[0]
            }
    }


@click.command()
@click.option('--env', type=click.Choice(['alpha', 'integration', 'prod']), required=True, prompt=True)
@click.option('--portfolio/--sov', '-p', is_flag=True, default=True, prompt=True)
@click.option('--exposure-id', type=int, prompt=True, required=True)
@click.option('--peril', type=str, prompt=True, required=True, callback=validate_peril)
@click.option('--measure', type=click.Choice(['tiv', 'gul', 'gel', 'gl']), callback=validate_measure)
@click.option('--name', type=str, required=True, prompt=True)
def cli(env, portfolio, exposure_id, peril, measure, name):
    print_arg('Env', env)
    print_arg('Portfolio', portfolio)
    print_arg('Exposure', exposure_id)
    print_arg('Peril', peril)
    print_arg('Measure', measure)
    script_field = get_script_field()
    spider_settings = get_spider_settings(exposure_id, portfolio, peril, measure)
    out = {
        'Script': script_field['Script'].replace('\n', ''),
        'AsyncSpiderAnalysis': spider_settings['AsyncSpiderAnalysis']
    }
    out_file = os.path.join(os.environ['RACE_TESTS_DIR'], f'{name}.json')
    with open(out_file, 'w') as f:
        json.dump(out, f, indent=4)
    print_arg('File', out_file)


if __name__ == '__main__':
    try:
        cli()
    except Exception as e:
        ic(e)

ic('end')
