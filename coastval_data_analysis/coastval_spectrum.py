import datetime
import numpy as np
import pandas as pd
import re
import scipy.io as sio
from bson.son import SON
from pymongo import MongoClient
from scipy import integrate
from scipy.interpolate import interp1d
from os.path import dirname, realpath, join
from os import remove

client = MongoClient()
client = MongoClient('localhost', 27017)
db = client['coastval_data_analysis-db']

dir_path = dirname(realpath(__file__))
# Calibration for the Sea Bird instrument
calibration = sio.loadmat(join(dir_path, "data/SeaBird_NewCalFiles2.mat"))
# The Sentinel 3 Spectral Response Function
data_sent_srf = sio.loadmat(join(dir_path, 'data/Sentinel3_SRF.mat'))
base_instrument = 'SATHPL0452'

def aggregate(datetime_start, datetime_end, max_angle=4):
    result = db.radiances.aggregate([

        {'$match': {'start_time': {'$gte': datetime_start, '$lt': datetime_end}}},
        {'$lookup':
             {'from': 'environment_stats',
              'localField': '_id',
              'foreignField': '_id',
              'as': 'environment'}},
        {
            '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$environment', 0]}, "$$ROOT"]}}
        },
        {'$lookup':
             {'from': 'tilt_stats',
              'localField': '_id',
              'foreignField': '_id',
              'as': 'tilt'}},
        {
            '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$tilt', 0]}, "$$ROOT"]}}
        },
        #         { "$match": {"instrument": { "$exists": "true", "$ne": "null"}}},
        {"$match": {"roll_max": {"$exists": "true", "$ne": "null"}}},
        {"$match": {"pitch_max": {"$exists": "true", "$ne": "null"}}},
        {"$match": {"pressure_mean": {"$exists": "true", "$ne": "null"}}},
        #         { "$match": {"start_time": { "$exists": "true", "$ne": "null"}}},
        #         { "$match": {"end_time": { "$exists": "true", "$ne": "null"}}},
        #         { "$match": {"integration_time": { "$exists": "true", "$ne": "null"}}},
        #         { "$match": {"dark_samples": { "$exists": "true", "$ne": "null"}}},
        #         { "$match": {"light_data": { "$exists": "true", "$ne": "null"}}},
        {'$match': {'roll_max': {'$lt': max_angle}}},
        #         {'$match': {'pitch_max': {'$lt': max_angle}}},
        {"$group":
            {
                #          "_id": {'timestamp_file': '$timestamp_file', 'idx': '$idx'},
                "_id": {'idx': '$instrument'},
                #          'timestamp_file': {'$push': '$timestamp_file'},
                #          'idx': {'$push': '$idx'},
                'roll': {'$push': '$roll_max'},
                'pitch': {'$push': '$pitch_max'},
                'temperature': {'$push': '$temp_mean'},
                'heading': {'$push': '$heading'},
                'pressure': {'$push': '$pressure_mean'},
                #          'instruments': {'$push': '$instrument'},
                'start_time': {'$push': '$start_time'},
                'end_time': {'$push': '$end_time'},
                'integration_time': {'$push': '$integration_time'},
                'dark_samples': {'$push': '$dark_samples'},
                'light_data': {'$push': '$light_data'}
            }
        },
        {"$sort": SON([("start_time", 1)])},
    ], allowDiskUse=True)
    return result


def days():
    res = db.radiances.aggregate([
        {"$project": {
             "year": {"$year": "$start_time"},
             "month": {"$month": "$start_time"},
             "day": {"$dayOfMonth": "$start_time"},
             "hour": {"$hour": "$start_time"}
        }
        },
        {"$group": {
            "_id": None,
            "distinctDate": {"$addToSet":
                                 {"year": "$year", "month": "$month", "day": "$day", "day": "$day" , "hour": "$hour"}
                             }
        }},
        {"$sort": SON([("distinctDate", 1)])},
    ], allowDiskUse=True)
    for d in next(res)['distinctDate']:
        yield datetime.datetime(d['year'], d['month'], d['day'], d['hour'])


def get_wavelenghts(instrument):
    instrument_num = re.search('.*(\d{3})', instrument).group(1)
    return calibration['cal' + instrument_num][:, 0]


def get_data(datetime_start, datetime_end):
    for data in aggregate(datetime_start, datetime_end):
        instrument = data['_id']['idx']
        del data['_id']

        light_df = pd.DataFrame.from_dict(data.pop('light_data'))
        vals_df = pd.DataFrame.from_dict(data)

        vals_df = vals_df.set_index('start_time')
        wavelengths = get_wavelenghts(instrument)
        light_df.index = vals_df.index
        light_df.columns = wavelengths
        yield instrument, {'vals': vals_df, 'light': light_df}


def heading_by_day():
    res = db.tilt.aggregate(
        [
            {"$group":
                {
                    "_id": {
                        'year': {'$year': "$timestamp"},
                        'month': {'$month': "$timestamp"},
                        'day': {'$dayOfMonth': "$timestamp"},
                        'hour': {'$hour': "$timestamp"}
                    },
                    'first': {'$first': '$heading'},
                },
            },
            #       { '$limit': 10 },
        ], allowDiskUse=True
    )
    heading_by_day_dict = {datetime.datetime(r['_id']['year'], r['_id']['month'], r['_id']['day'], r['_id']['hour']):
                               {'first': r['first']}
                           for r in res}
    df_headings = pd.DataFrame.from_dict(heading_by_day_dict, orient='index')
    df_headings = ((df_headings - 180) % 360) + 180
    return df_headings

"""
Functions to compute the CoastVal-derived radiance
"""


def get_instrument_number(instrument_name):
    return int(re.match('[A-Z]*(\d{4})', instrument_name).group(1))


# def get_calibration_coeffs(inst_num, indx):
#     return pd.Series(calibration['cal' + str(inst_num)][:,1], index=index)

def calibrate_spectrum(df, instrument, inst_num):
    """ Returns the calibrated radiance, as given in the Satlantic Instrument
    File Standard document SAT-DN-00134.
    """
    index = df[instrument]['light'].columns
    c2 = pd.Series(calibration['cal' + str(inst_num)][:, 1], index=index)
    c3 = pd.Series(calibration['cal' + str(inst_num)][:, 2], index=index)
    c4 = pd.Series(calibration['cal' + str(inst_num)][:, 3], index=index)
    c5 = pd.Series(calibration['cal' + str(inst_num)][:, 4], index=index)

    itime = df[instrument]['vals'].integration_time / 1000  # integration time in seconds
    spec = df[instrument]['light']
    return (c3 * c4 * (spec - c2) * c5.iloc[14]).divide(itime, axis=0)


def radiance(df, instrument_sp, instrument_dk):
    """Calibrate and subtract the dark frame"""
    assert (df[instrument_sp]['vals'].index == df[instrument_sp]['light'].index).all()
    assert (df[instrument_dk]['vals'].index == df[instrument_dk]['vals'].index).all()
    sp_num, dk_num = get_instrument_number(instrument_sp), get_instrument_number(instrument_dk)
    assert sp_num == dk_num
    radw2 = calibrate_spectrum(df, instrument_sp, sp_num)
    dradw2 = calibrate_spectrum(df, instrument_dk, dk_num)  # dark frame
    radiance_w2 = radw2 - dradw2
    return radiance_w2


"""
Functions for Compute the spectra as measured by Sentinel-3 (using it's Spectral Response Functions)
"""


def interp_msrf(band_num):
    """Returns the Spectral Response Function as a function (as opposed to) the discrete values,
    so that it can be resample to at the wavelengths of the CoastVal spectra.
    """
    return interp1d(data_sent_srf['MSRFwavelength'][:, band_num], data_sent_srf['MSRF'][:, band_num],
                    fill_value=0,
                    bounds_error=False,
                    kind='nearest')


def measured_by_sentinel3_band(spec_df, band, msrf_dict):
    """ Multiply the Spectral Response Function and the CoastVal-derived spectra together and intergrate
    over the spectra to get the radiance measured by Sentinel-3.
    """
    wavelengths = spec_df.columns
    spec = spec_df
    # Spectral Response Function sampled at the wavelenght of the CoastVal Instrument
    msrf = pd.Series(msrf_dict[band](wavelengths), index=spec.columns)

    def multiply_and_integrate_spec(spec):
        _radiance = integrate.simps(spec * msrf, wavelengths)
        normalize = integrate.simps(msrf, wavelengths)
        return _radiance / normalize

    # Apply function to each spectrum in the timeseries of spectra
    measured_spectra = spec_df.apply(multiply_and_integrate_spec, axis=1)
    return measured_spectra


def spectra_measured_by_sentinel3(spec_df, wavelengths_centre, msrf_dict):
    """The radiance as measured by Sentinel-3.
    The radiance that "would be" measured by Sentinel-3 is computed by multilpying the Spectral Response Function
    by the radiance measured by Coastval for each Sentinel-3 band.
    """
    return pd.DataFrame({wavelengths_centre[band - 1]:
                             measured_by_sentinel3_band(spec_df, band, msrf_dict) for band in range(1, 17)})


def reindex_dataframe(data):
    """
    Put all the data from each instrument on the same index
    """

    data_base = data.pop(base_instrument)
    new_index = data_base['light'].index
    for instrument in data.keys():
        for k in ['light', 'vals']:
            data[instrument][k] = data[instrument][k].reindex(new_index,
                                                              method='nearest',
                                                              tolerance=datetime.timedelta(seconds=20))
    data[base_instrument] = data_base
    return data


def compute_spectra(start, end):
    radiance_dict = {
        'tradiance_w2': ['SATHPL0609', 'SATPLD0609'],
        'tradiance_w1': ['SATHPL0452', 'SATPLD0452'],
        'tirradiance_a': ['SATHSE0612', 'SATHED0612'],
        'tirradiance_w1': ['SATHPE0332', 'SATPED0332'],
    }

    # The names of the spectra in the Octave code
    radiance_names = dict(tradiance_w2='spctrm1',
                          tirradiance_a='spctrm3',
                          tirradiance_w1='spctrm4',
                          tradiance_w1='spctrm5')

    data = {instrument: datas for instrument, datas in get_data(start, end)}
    df = reindex_dataframe(data)

    # Compute the radiances & irradiances, with the arguments given in radiance_dict.
    # The radiance is computed for each spectra in the timeseries in the dataframes.
    spectra = {rad_name: radiance(df, instrument1, instrument2)
                    for rad_name, (instrument1, instrument2) in radiance_dict.items()}
    # spectra = pd.concat(spectra, axis=1)
    # Load the Sentinel-3 Spectral Response Functions

    wavelengths_centre_exact = data_sent_srf['SRFF'].flatten()
    wavelengths_centre = np.round(wavelengths_centre_exact, 2).astype(np.float16)
    # Spectral response interpolation-function dictionary for each band
    msrf_dict = {i + 1: interp_msrf(i) for i in range(data_sent_srf['MSRFwavelength'].shape[1])}

    # Compute the radiance/irradiances. Get a dictionary of timeseries of these spectra
    spec_s3bands = {spec_name: spectra_measured_by_sentinel3(spectra[spec_name], wavelengths_centre, msrf_dict)
                    for spec_name in radiance_dict.keys()}

    # Compute the upwelling radiance just below surface
    dz1 = 2
    KLus = (np.abs(np.log(spec_s3bands['tradiance_w1'] / spec_s3bands['tradiance_w2'])) / 1.475)
    KLu = KLus.mean(axis=0)
    LunOminus = spec_s3bands['tradiance_w1'] * np.exp(KLu * dz1)

    # Compute the water-leaving radiance
    etaw = 1.34  # calculated refractive index for input salinity and temperature for each wavelength
    Tf = 0.97  # Fresnel reflectance will be different and must be calculated
    # for lower solar elevation angles. That is, for SEA ~45 Tf = ~0.96, or fpr
    # SEA ~30 deg. Tf = 0.93
    Tws = Tf / etaw ** 2
    Lwn = Tws * LunOminus

    # Remote sensing reflectance
    Rrs = (Lwn / spec_s3bands['tirradiance_a']) * np.pi

    info = {'info': df[base_instrument]['vals']}
    return pd.concat({'KLus': KLus, 'LunOminus': LunOminus, 'Lwn': Lwn, 'Rrs': Rrs, **spectra, **info}, axis=1)


if __name__ == '__main__':
    try:
        remove('data/spectra_calibrated2.h5')
    except FileNotFoundError:
        pass
    store = pd.HDFStore('data/spectra_calibrated2.h5')
    for t_start in sorted(days()):
        try:
            print(t_start)
            t_end = t_start + datetime.timedelta(hours=1)
            all_specs = compute_spectra(t_start, t_end)
            store.append('spectra_calibrated', all_specs, complevel=5)
        except (KeyError, ValueError):
            print(f'Skipping {t_start}')
