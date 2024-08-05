import pandas as pd
import pyodbc
import os

# Paths
base_folder = 'C:/Users/fmohammed7/Downloads/stooq_historical_US_stock_data/data/daily/us/nyse ETFS'
connection_string = (
'Driver={ODBC Driver 18 for SQL Server};'
    'Server=PLAN2007576-\SQLSERVER_NEW;'
    'Database=STOOQ_DATA_USA;'
    'UID=sa;'
    'PWD=Admin321*;'
    'TrustServerCertificate=yes;'
)
table_name = 'dbo.DailyOHLCV'
log_file_path = 'processed_files.log'
high_trust_etfs_stocks = [
    'aapl.us.txt', 'msft.us.txt', 'amzn.us.txt', 'nvda.us.txt', 'googl.us.txt', 'goog.us.txt',
    'meta.us.txt', 'brkb.us.txt', 'tsla.us.txt', 'unh.us.txt', 'jpm.us.txt', 'jnj.us.txt',
    'v.us.txt', 'wmt.us.txt', 'bac.us.txt', 'pg.us.txt', 'xom.us.txt', 'hd.us.txt',
    'dis.us.txt', 'pypl.us.txt', 'vz.us.txt', 'cmcsa.us.txt', 'adbe.us.txt', 'csco.us.txt',
    'ko.us.txt', 'pep.us.txt', 'mrk.us.txt', 'abbv.us.txt', 'll.y.us.txt', 'avgo.us.txt',
    'ma.us.txt', 'pfe.us.txt', 'intc.us.txt', 'cvx.us.txt', 'abt.us.txt', 'mdt.us.txt',
    'crm.us.txt', 'tmo.us.txt', 'bmy.us.txt', 'c.us.txt', 'amgn.us.txt', 'gild.us.txt',
    'nke.us.txt', 'cost.us.txt', 'lmt.us.txt', 'mcd.us.txt', 'mmm.us.txt', 'gs.us.txt',
    'hon.us.txt', 'lin.us.txt', 'bkng.us.txt', 'wfc.us.txt', 'txn.us.txt', 'ba.us.txt',
    'elv.us.txt', 'axp.us.txt', 'low.us.txt', 'tgt.us.txt', 'pld.us.txt', 'adi.us.txt',
    'ci.us.txt', 'usb.us.txt', 'sny.us.txt', 'mdc.us.txt', 'fdx.us.txt', 'nee.us.txt',
    'cat.us.txt', 'exxon.us.txt', 'xle.us.txt', 'pxt.us.txt', 'sbu.us.txt', 'mtch.us.txt',
    'ebay.us.txt', 'mdlz.us.txt', 'stz.us.txt', 'apd.us.txt', 'cl.us.txt', 'met.us.txt',
    'alxn.us.txt', 'regn.us.txt', 'unh.us.txt', 'gsk.us.txt', 'bsx.us.txt', 'cvs.us.txt',
    'f.us.txt', 'de.us.txt', 'ge.us.txt', 'bhp.us.txt', 'ko.us.txt', 'snps.us.txt',
    'adp.us.txt', 'intc.us.txt', 'ibm.us.txt', 'yum.us.txt', 'dhr.us.txt', 'mu.us.txt',
    't.us.txt', 'amd.us.txt', 'sq.us.txt', 'twtr.us.txt', 'z.us.txt', 'snap.us.txt', 'baba.us.txt',
    'bidu.us.txt', 'pdd.us.txt', 'jd.us.txt', 'nio.us.txt', 'li.us.txt', 'xpev.us.txt',
    'tsm.us.txt', 'ntes.us.txt', 'baidu.us.txt', 'bili.us.txt', 'iq.us.txt', 'vipshop.us.txt',
    'vip.us.txt', 'weibo.us.txt', 'wb.us.txt', 'qutoutiao.us.txt', 'lkm.us.txt', 'huya.us.txt',
    'tcehy.us.txt', 'tencent.us.txt', 'meituan.us.txt', 'alibaba.us.txt', 'baid.us.txt',
    'pdd.us.txt', 'taobao.us.txt', 'jd.com.us.txt', 'jd.us.txt', 'nordstrom.us.txt', 'ko.us.txt',
    'hsbc.us.txt', 'unilever.us.txt', 'hdb.us.txt', 'infy.us.txt', 'wipro.us.txt', 'twilio.us.txt',
    'shopify.us.txt', 'spot.us.txt', 'spotify.us.txt', 'doordash.us.txt', 'dash.us.txt', 'lyft.us.txt',
    'uber.us.txt', 'airbnb.us.txt', 'abnb.us.txt', 'zoom.us.txt', 'zm.us.txt', 'slack.us.txt', 'work.us.txt',
    'roku.us.txt', 'pinterest.us.txt', 'pins.us.txt', 'ebay.us.txt', 'paypal.us.txt', 'pypl.us.txt', 'coinbase.us.txt',
    'coin.us.txt', 'robinhood.us.txt', 'hood.us.txt', 'sofi.us.txt', 'palantir.us.txt', 'pltr.us.txt', 'snowflake.us.txt',
    'snow.us.txt', 'cloudflare.us.txt', 'net.us.txt', 'docusign.us.txt', 'docu.us.txt', 'crowdstrike.us.txt', 'crwd.us.txt',
    'okta.us.txt', 'octa.us.txt', 'fortinet.us.txt', 'ftnt.us.txt', 'servicenow.us.txt', 'now.us.txt',
    # Additional 100 tickers
    'algn.us.txt', 'alxn.us.txt', 'are.us.txt', 'biib.us.txt', 'bhp.us.txt', 'blk.us.txt',
    'cmg.us.txt', 'cnc.us.txt', 'cof.us.txt', 'cpb.us.txt', 'ctsh.us.txt', 'cvs.us.txt',
    'd.us.txt', 'dhi.us.txt', 'dltr.us.txt', 'duk.us.txt', 'dxcm.us.txt', 'eog.us.txt',
    'eqix.us.txt', 'exc.us.txt', 'expd.us.txt', 'f.us.txt', 'fcx.us.txt', 'gd.us.txt',
    'gm.us.txt', 'hlf.us.txt', 'hsy.us.txt', 'hca.us.txt', 'intu.us.txt', 'ko.us.txt',
    'kmi.us.txt', 'lhx.us.txt', 'lnk.us.txt', 'mdlz.us.txt', 'mu.us.txt', 'nflx.us.txt',
    'ntap.us.txt', 'ntr.us.txt', 'okta.us.txt', 'pfg.us.txt', 'ph.us.txt', 'pll.us.txt',
    'pru.us.txt', 'sbux.us.txt', 'sna.us.txt', 'stx.us.txt', 'swn.us.txt', 'symc.us.txt',
    'tel.us.txt', 'tdg.us.txt', 'tsco.us.txt', 'ups.us.txt', 'uri.us.txt', 'vrsn.us.txt',
    'wba.us.txt', 'wdc.us.txt', 'wmb.us.txt', 'wmt.us.txt', 'xlm.us.txt', 'xlnx.us.txt',
    'zbra.us.txt', 'zts.us.txt', 'yumc.us.txt', 'zm.us.txt', 'zyxi.us.txt', 'asml.us.txt',
    'adsk.us.txt', 'amzn.us.txt', 'avy.us.txt', 'atvi.us.txt', 'baz.us.txt', 'clx.us.txt',
    'ctl.us.txt', 'dte.us.txt', 'duke.us.txt', 'emr.us.txt', 'exp.us.txt', 'fnv.us.txt',
    'gme.us.txt', 'hig.us.txt', 'ibm.us.txt', 'idxx.us.txt', 'ice.us.txt', 'itw.us.txt',
    'jnj.us.txt', 'khc.us.txt', 'kr.us.txt', 'lrcx.us.txt', 'mco.us.txt', 'moh.us.txt',
    'msi.us.txt', 'ndsn.us.txt', 'nke.us.txt', 'nvr.us.txt', 'orcl.us.txt', 'pcar.us.txt',
    'pld.us.txt', 'rmd.us.txt', 'sbac.us.txt', 'shw.us.txt', 'sky.us.txt', 'sre.us.txt',
    'tfx.us.txt', 'trow.us.txt', 'udr.us.txt', 'vfc.us.txt', 'vrtx.us.txt', 'xom.us.txt',
    'apd.us.txt', 'asgn.us.txt', 'carr.us.txt', 'cdw.us.txt', 'clh.us.txt', 'crl.us.txt',
    'ctas.us.txt', 'evr.us.txt', 'ffiv.us.txt', 'flo.us.txt', 'ftnt.us.txt', 'gra.us.txt',
    'grmn.us.txt', 'har.us.txt', 'hsic.us.txt', 'hubg.us.txt', 'irt.us.txt', 'jkhy.us.txt',
    'jll.us.txt', 'jwa.us.txt', 'masi.us.txt', 'mdc.us.txt', 'mtd.us.txt', 'myl.us.txt',
    'navi.us.txt', 'nycb.us.txt', 'ory.us.txt', 'patk.us.txt', 'pdco.us.txt', 'pki.us.txt',
    'slgn.us.txt', 'td.us.txt', 'tti.us.txt', 'tpx.us.txt', 'umc.us.txt', 'zen.us.txt'
]
high_trust_nasdaq_etfs = [
    'aadr.us.txt', 'aaxj.us.txt', 'acwi.us.txt', 'acwx.us.txt', 'agng.us.txt', 'agzd.us.txt',
    'aia.us.txt', 'aiq.us.txt', 'airr.us.txt', 'alty.us.txt', 'angl.us.txt', 'aqwa.us.txt',
    'aset.us.txt', 'bbh.us.txt', 'bfit.us.txt', 'bgrn.us.txt', 'bib.us.txt', 'bis.us.txt',
    'bjk.us.txt', 'bkch.us.txt', 'blcn.us.txt', 'bnd.us.txt', 'bndw.us.txt', 'bndx.us.txt',
    'botz.us.txt', 'brrr.us.txt', 'bsco.us.txt', 'bscp.us.txt', 'bscq.us.txt', 'bscr.us.txt',
    'bscs.us.txt', 'bsct.us.txt', 'bscu.us.txt', 'bsjo.us.txt', 'bsjp.us.txt', 'bsjq.us.txt',
    'bsjr.us.txt', 'bsjs.us.txt', 'bsmo.us.txt', 'bsmp.us.txt', 'bsmq.us.txt', 'bsmr.us.txt',
    'bsms.us.txt', 'bsmt.us.txt', 'bsmu.us.txt', 'btec.us.txt', 'bug.us.txt', 'cacg.us.txt',
    'carz.us.txt', 'cath.us.txt', 'cdc.us.txt', 'cdl.us.txt', 'cefa.us.txt', 'cfa.us.txt',
    'cfo.us.txt', 'chb.us.txt', 'cibr.us.txt', 'cid.us.txt', 'cil.us.txt', 'ciz.us.txt',
    'clou.us.txt', 'cncr.us.txt', 'comt.us.txt', 'csa.us.txt', 'csb.us.txt', 'csf.us.txt',
    'csml.us.txt', 'ctec.us.txt', 'cxse.us.txt', 'dali.us.txt', 'dapp.us.txt', 'dax.us.txt',
    'ddiv.us.txt', 'demz.us.txt', 'dgre.us.txt', 'dgrs.us.txt', 'dgrw.us.txt', 'dmxf.us.txt',
    'driv.us.txt', 'dvlu.us.txt', 'dvol.us.txt', 'dvy.us.txt', 'dwas.us.txt', 'dwaw.us.txt',
    'dwsh.us.txt', 'dwus.us.txt', 'dxjs.us.txt', 'ebiz.us.txt', 'ecow.us.txt', 'edoc.us.txt',
    'eema.us.txt', 'efas.us.txt', 'emb.us.txt', 'emcb.us.txt', 'emif.us.txt', 'emxc.us.txt',
    'emxf.us.txt', 'enzl.us.txt', 'eqrr.us.txt', 'ersx.us.txt', 'esgd.us.txt', 'esge.us.txt',
    'esgu.us.txt', 'espo.us.txt', 'eufn.us.txt', 'ewjv.us.txt', 'ewzs.us.txt', 'faar.us.txt',
    'fab.us.txt', 'fad.us.txt', 'faln.us.txt', 'fbz.us.txt', 'fca.us.txt', 'fcal.us.txt',
    'fcef.us.txt', 'fcvt.us.txt', 'fdni.us.txt', 'fdt.us.txt', 'fdts.us.txt', 'fem.us.txt',
    'femb.us.txt', 'fems.us.txt', 'fep.us.txt', 'feuz.us.txt', 'fex.us.txt', 'fgm.us.txt',
    'fics.us.txt', 'fid.us.txt', 'finx.us.txt', 'fixd.us.txt', 'fjp.us.txt', 'fku.us.txt',
    'fln.us.txt', 'fmb.us.txt', 'fmhi.us.txt', 'fnk.us.txt', 'fnx.us.txt', 'fny.us.txt',
    'fpa.us.txt', 'fpxe.us.txt', 'fpxi.us.txt', 'fsz.us.txt', 'fta.us.txt', 'ftag.us.txt',
    'ftc.us.txt', 'ftcs.us.txt', 'ftgc.us.txt', 'fthi.us.txt', 'ftri.us.txt', 'ftsl.us.txt',
    'ftsm.us.txt', 'ftxg.us.txt', 'ftxh.us.txt', 'ftxl.us.txt', 'ftxn.us.txt', 'ftxo.us.txt',
    'ftxr.us.txt', 'fv.us.txt', 'fvc.us.txt', 'fyc.us.txt', 'fyt.us.txt', 'fyx.us.txt',
    'gldi.us.txt', 'gnma.us.txt', 'gnom.us.txt', 'grid.us.txt', 'gxtg.us.txt', 'herd.us.txt',
    'hero.us.txt', 'hewg.us.txt', 'hlal.us.txt', 'hndl.us.txt', 'hyls.us.txt', 'hyxf.us.txt',
    'hyzd.us.txt', 'ibb.us.txt', 'ibit.us.txt', 'ibte.us.txt', 'ibtf.us.txt', 'ibtg.us.txt',
    'ibth.us.txt', 'ibti.us.txt', 'ibtj.us.txt', 'ibtk.us.txt', 'icln.us.txt', 'ief.us.txt',
    'iei.us.txt', 'ieus.us.txt', 'ifgl.us.txt', 'ifv.us.txt', 'igf.us.txt', 'igib.us.txt',
    'igov.us.txt', 'igsb.us.txt', 'ihyf.us.txt', 'ijt.us.txt', 'imcv.us.txt', 'indy.us.txt',
    'ipkw.us.txt', 'ishg.us.txt', 'ishp.us.txt', 'istb.us.txt', 'ius.us.txt', 'iusb.us.txt',
    'iusg.us.txt', 'iusv.us.txt', 'ixus.us.txt', 'jsmd.us.txt', 'jsml.us.txt', 'kbwb.us.txt',
    'kbwd.us.txt', 'kbwp.us.txt', 'kbwr.us.txt', 'kbwy.us.txt', 'krma.us.txt', 'ldem.us.txt',
    'ldsf.us.txt', 'legr.us.txt', 'lmbs.us.txt', 'lrge.us.txt', 'lvhd.us.txt', 'mbb.us.txt',
    'mchi.us.txt', 'mdiv.us.txt', 'miln.us.txt', 'nfty.us.txt', 'nvd.us.txt', 'nxtg.us.txt',
    'oneq.us.txt', 'pdbc.us.txt', 'pdp.us.txt', 'pey.us.txt', 'pez.us.txt', 'pff.us.txt',
    'pfi.us.txt', 'pfm.us.txt', 'pgj.us.txt', 'pho.us.txt', 'pid.us.txt', 'pie.us.txt',
    'pio.us.txt', 'piz.us.txt', 'pkw.us.txt', 'pnqi.us.txt', 'potx.us.txt', 'pph.us.txt',
    'prfz.us.txt', 'prn.us.txt', 'psc.us.txt', 'pscc.us.txt', 'pscd.us.txt', 'psce.us.txt',
    'pscf.us.txt', 'psch.us.txt', 'psci.us.txt', 'pscm.us.txt', 'psct.us.txt', 'pscu.us.txt',
    'pset.us.txt', 'psl.us.txt', 'ptf.us.txt', 'pth.us.txt', 'pui.us.txt', 'pxi.us.txt',
    'py.us.txt', 'pyz.us.txt', 'qaba.us.txt', 'qat.us.txt', 'qcln.us.txt', 'qqew.us.txt',
    'qqq.us.txt', 'qqqj.us.txt', 'qqqm.us.txt', 'qqqn.us.txt', 'qqxt.us.txt', 'qtec.us.txt',
    'qyld.us.txt', 'qylg.us.txt', 'rdvy.us.txt', 'reit.us.txt', 'rfdi.us.txt', 'rfem.us.txt',
    'rfeu.us.txt', 'ring.us.txt', 'rnem.us.txt', 'rnlc.us.txt', 'rnmc.us.txt', 'rnrg.us.txt',
    'rnsc.us.txt', 'robt.us.txt', 'rth.us.txt', 'scz.us.txt', 'sdg.us.txt', 'sdvy.us.txt',
    'shv.us.txt', 'shy.us.txt', 'skor.us.txt', 'skyu.us.txt', 'skyy.us.txt', 'slqd.us.txt',
    'slvo.us.txt', 'smcp.us.txt', 'smh.us.txt', 'snsr.us.txt', 'socl.us.txt', 'soxx.us.txt',
    'sqlv.us.txt', 'sqqq.us.txt', 'sret.us.txt', 'susb.us.txt', 'susc.us.txt', 'susl.us.txt',
    'tdiv.us.txt', 'tlt.us.txt', 'tqqq.us.txt', 'tur.us.txt', 'uae.us.txt', 'ucyb.us.txt',
    'ufo.us.txt', 'uivm.us.txt', 'ulvm.us.txt', 'usig.us.txt', 'usmc.us.txt', 'usxf.us.txt',
    'vcit.us.txt', 'vclt.us.txt', 'vcsh.us.txt', 'vgit.us.txt', 'vglt.us.txt', 'vgsh.us.txt',
    'vigi.us.txt', 'vmbs.us.txt', 'vnqi.us.txt', 'vone.us.txt', 'vong.us.txt', 'vonv.us.txt',
    'vrig.us.txt', 'vsda.us.txt', 'vsmv.us.txt', 'vtc.us.txt', 'vthr.us.txt', 'vtip.us.txt',
    'vtwg.us.txt', 'vtwo.us.txt', 'vtwv.us.txt', 'vwob.us.txt', 'vxus.us.txt', 'vymi.us.txt',
    'wbnd.us.txt', 'wcbr.us.txt', 'wcld.us.txt', 'winc.us.txt', 'wood.us.txt', 'xt.us.txt',
    'ylde.us.txt'
]

high_trusted_nyse_etfs=[
'aaa.us.txt'
,'aaau.us.txt'
,'aapx.us.txt'
,'actv.us.txt'
,'adme.us.txt'
,'aesr.us.txt'
,'afif.us.txt'
,'agg.us.txt'
,'agih.us.txt'
,'agz.us.txt'
,'ahoy.us.txt'
,'aieq.us.txt'
,'altl.us.txt'
,'amdy.us.txt'
,'amlp.us.txt'
,'aqwa.us.txt'
,'ashb.us.txt'
,'atfv.us.txt'
,'axsm.us.txt'
,'azed.us.txt'
,'bab.us.txt'
,'bbb.us.txt'
,'bdcl.us.txt'
,'bfz.us.txt'
,'bhy.us.txt'
,'bibl.us.txt'
,'bil.us.txt'
,'bits.us.txt'
,'bitx.us.txt'
,'bkch.us.txt'
,'bld.us.txt'
,'bmed.us.txt'
,'bmhr.us.txt'
,'bno.us.txt'
,'bpod.us.txt'
,'brk.us.txt'
,'bsco.us.txt'
,'btcl.us.txt'
,'bufr.us.txt'
,'cbio.us.txt'
,'cfo.us.txt'
,'cgh.us.txt'
,'chil.us.txt'
,'cibr.us.txt'
,'clou.us.txt'
,'corn.us.txt'
,'cpa.us.txt'
,'cppi.us.txt'
,'cqqq.us.txt'
,'cryo.us.txt'
]
def test_DB_connection(connection_string):
    try:
        conn = pyodbc.connect(connection_string)
        print("Connection successful!")
        conn.close()
    except Exception as e:
        print("Error:", e)

def read_and_transform(file_path, exchange_name, exchange_type):
    # Check if the file is empty
    if os.stat(file_path).st_size == 0:
        print(f"File {file_path} is empty. Skipping.")
        return None

    # Read the data from the text file
    df = pd.read_csv(file_path)
    # Rename columns to match your database schema
    df.columns = ['Ticker', 'Period', 'TradeDate', 'TradeTime', 'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice',
                  'Volume', 'OpenInterest']
    # Add ExchangeName and ExchangeType columns
    df['ExchangeName'] = exchange_name
    df['ExchangeType'] = exchange_type
    # Convert TradeDate and TradeTime to appropriate types if necessary
    df['TradeDate'] = pd.to_datetime(df['TradeDate'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

    # Convert TradeTime from '000000' to '00:00:00'
    def format_time(time_str):
        time_str = str(time_str).zfill(6)
        return f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"

    df['TradeTime'] = df['TradeTime'].apply(format_time)


    df['OpenPrice'] = df['OpenPrice'].astype(float)
    df['HighPrice'] = df['HighPrice'].astype(float)
    df['LowPrice'] = df['LowPrice'].astype(float)
    df['ClosePrice'] = df['ClosePrice'].astype(float)
    df['Volume'] = df['Volume'].astype(float)
    df['OpenInterest'] = df['OpenInterest'].fillna(0).astype(int)  # Fill NA values with 0 and convert to int
    # print(df.dtypes)
    print(df.head())
    return df


def insert_data_to_sql(df, table_name, connection_string):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    for index, row in df.iterrows():
        cursor.execute('''
        INSERT INTO STOOQ_DATA_USA.dbo.DailyOHLCV (ExchangeName, ExchangeType, Ticker, Period, TradeDate, TradeTime, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, OpenInterest)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                       row['ExchangeName'], row['ExchangeType'], row['Ticker'], row['Period'], row['TradeDate'],
                       row['TradeTime'],
                       row['OpenPrice'], row['HighPrice'], row['LowPrice'], row['ClosePrice'], row['Volume'],
                       row['OpenInterest'])

    conn.commit()
    cursor.close()
    conn.close()




# Function to determine exchange name and type from folder structure
def get_exchange_info(folder_name):
    parts = folder_name.split()
    exchange_name = parts[0].upper()
    exchange_type = parts[1].capitalize()
    return exchange_name, exchange_type

def get_processed_files(log_file_path):
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r') as log_file:
            processed_files = set(line.strip() for line in log_file)
    else:
        processed_files = set()
    return processed_files

def log_processed_file(log_file_path, file_name):
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"{file_name}\n")


#Test our connection to the DB
test_DB_connection(connection_string)

processed_files = get_processed_files(log_file_path)

# Process and insert data
for root, dirs, files in os.walk(base_folder):

    for file in files:
        if file.endswith('.txt') and file in high_trusted_nyse_etfs and file not in processed_files :
            file_path = os.path.join(root, file)
            folder_name=os.path.basename(root)
            exchange_name, exchange_type = get_exchange_info(folder_name)
            print(f"Start inserting data from {file}")
            print(exchange_name)
            print(exchange_type)
            df = read_and_transform(file_path, exchange_name, exchange_type)
            if df is not None:
                insert_data_to_sql(df, table_name, connection_string)
                print(f"Inserted data from {file}")
                log_processed_file(log_file_path, file)
            else:
                print(f"SKIPPING File -{file} ==> inelgible to upload! ")
