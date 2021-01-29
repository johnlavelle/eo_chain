################################################################################
# NOTE : THIS IS FOR FIRMAWARE VERSION 1.951 --- BETA!!!
################################################################################

# uWaveII Directional Wave Sensor
# Philip Trickett / Shane Ormonde / Glenn Tarpey
# 12th January 2018
# 30th September 2019
# 11th November 2020
# 8th December 2020
# 10th December 2020 - Fuck this shit
# 17th December 2020 - I'm out
# uwave_2.py

# 1) set polling 1
# 2) set outputser 0195F7E7

# For 4Hz sampling rate (equivalent to A 0.25 second timestep), please do the following:
# SET INTERVAL 10000
# SET AVR 25

# Set LOG F   - for 4 logs on uWave SD card
#

# NMEA,heading,Hs,DominantPeriodFW,AveragePeriodTz,AveragePeriodTe,AveragePeriodAPD,WaveDirectionFW,MeanWaveDir,Hmax,
# Hmax2,Pmax,a1,b1,a2,b2,accN,accE,accU,accX,accY,accZ,angR,angP,magX,magY,magZ,maxAccX,maxAccXIndex,maxAccY,
# maxAccYIndex,maxAccZ,maxAccZIndex,identity,date time,index*chk


# GET ALL - should give the following :

"""
TIME    	16:10:24 12/17/2020
IDENTITY 	SVS-603
VOLTAGE 	11.56
VERSION 	1.96beta2
BAUDSER 	9600
AVR     	25
INTERVAL	10000
TSTEP   	0.2500
COFF    	20.00
FILTERTYPE 	1
FILTERCOFF 	6.670
FILTERPREFAC  	13.000
EFRAMEXY	1
EFRAMEZ 	1
EXTCOMPASS 	0
EXTIMU   	0
DANG    	0.00
OFFSETZ 	2.00
OUTPUTUSB	00000000
OUTPUTSER	0195F7E7
LOG     	0F
FREQBINS	128
POLLING 	0
SPECUSB 	0
SPECSER 	1
SPECHDR 	1
DIRSPECUSB 	0
DIRSPECSER 	1
DIRSPECALPHA 	0.1000
DIRSPECLEN 	30
DIRSPECMOD 	1
VERTDISPUSB 	0
VERTDISPSER 	1
RECAL   	0
SUMMARYUSB	1
SUMMARYSER	0
RESETCOUNT	0
MAXTILTANGLE	30
DIRAVRFAC	21
PERIODFAC	21
PERIODFWMODEL	0
PERIODTHRESH	10.00
PERIODMINFAC	5.00
DIRECTTHRESH	80.00
DIRECTMINFAC	40.00
MEANDIRTHRESH	80.00
MEANDIRMINFAC	40.00
RAOVERTICAL	1.00000000 1.00000000 1.00000000 1.00000000 1.00000000 1.00000000 1.00000000 1.00000000 1.00000000 1.00000000
RAOVERTICALPER	2.00 4.00 6.00 8.00 10.00 12.00 14.00 16.00 18.00 20.00
RAOVERTICALSIZE	1
RAOVERTICALMAXP	20.00
RAOVERTICALMINP	2.00
RAOVERTICALTYPE	0
CALSTATUS	0330
SD CARD READY
OUTPUTUSB HEADER:
NO OUTPUT
OUTPUTSER HEADER:
NMEA,heading,Hs,DominantPeriodFW,AveragePeriodTz,AveragePeriodTe,AveragePeriodAPD,WaveDirectionFW,MeanWaveDir,Hmax,Hmax2,Pmax,a1,b1,a2,b2,accN,accE,accU,accX,accY,accZ,angR,angP,magX,magY,magZ,maxAccX,maxAccXIndex,maxAccY,maxAccYIndex,maxAccZ,maxAccZIndex,identity,date time,index*chk
SD CARD LOG FILES:
AC DS MG WA
"""

################################################################################

import time, math, subprocess, re
from octopus import rpc, Errors, connectors, utils
import handler
import math
import datetime
import os


class uWave2(handler.Handler):
    """Planet Ocean uWaveII Wave Sensor

    Ascii code:
     - "uwave_2"

     """

    equipment_type = "uwave_2"

    def __init__(self, deployment_id, deployment_name):
        super(uWave2, self).__init__(deployment_id, deployment_name)

        self.addProperty(20, "heading", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(21, "sig_wave", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(22, "dominant_periodFW", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(23, "dominant_waveDirFW", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(24, "h_max", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(25, "h_max2", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(26, "avr_periodTz", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(27, "avr_periodTe", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(28, "avr_periodAPD", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(29, "mean_waveDir", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(30, "p_max", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(31, "a1", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(32, "b1", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(33, "a2", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(34, "b2", float, handler.PROP_SENSOR, 0.0)
        # self.addProperty(35, "acc_n"      , float, handler.PROP_SENSOR, 0)
        # self.addProperty(36, "acc_e"      , float, handler.PROP_SENSOR, 0)
        # self.addProperty(37, "acc_u"      , float, handler.PROP_SENSOR, 0)
        # self.addProperty(39, "acc_x"      , float, handler.PROP_SENSOR, 0)
        # self.addProperty(40, "acc_y"      , float, handler.PROP_SENSOR, 0)
        # self.addProperty(41, "acc_z"      , float, handler.PROP_SENSOR, 0)
        self.addProperty(42, "roll", float, handler.PROP_SENSOR, 0)
        self.addProperty(43, "pitch", float, handler.PROP_SENSOR, 0)
        # self.addProperty(44, "mag_x", float, handler.PROP_SENSOR, 0)
        # self.addProperty(45, "mag_y", float, handler.PROP_SENSOR, 0)
        # self.addProperty(46, "mag_z", float, handler.PROP_SENSOR, 0)
        # self.addProperty(47, "maxAccX"   , float, handler.PROP_SENSOR, 0)
        # self.addProperty(48, "maxAccY"   , float, handler.PROP_SENSOR, 0)
        # self.addProperty(49, "maxAccZ"   , float, handler.PROP_SENSOR, 0)
        self.addProperty(50, "datetime", str, handler.PROP_SENSOR, '0')  # contains date + time
        # self.addProperty(51, "index"      , int, handler.PROP_SENSOR, 0)
        self.addProperty(52, "wdir_sd", float, handler.PROP_SENSOR, 0)
        self.addProperty(53, "hs_third", float, handler.PROP_SENSOR, 0)
        self.addProperty(54, "N", float, handler.PROP_SENSOR, 0.0)

        """
        self.addProperty(55, "voltage"    , str, handler.PROP_STATUS, '0')
        self.addProperty(56, "outputser"  , str, handler.PROP_STATUS, '0')
        self.addProperty(57, "resetcount" , str, handler.PROP_STATUS, '0')
        self.addProperty(58, "cal_status" , str, handler.PROP_STATUS, '0')
        self.addProperty(59, "polling"    , str, handler.PROP_STATUS, '0')
        self.addProperty(60, "summaryser" , str, handler.PROP_STATUS, '0')
        self.addProperty(61, "ser_header" , str, handler.PROP_STATUS, '0')
        """

    def initConnector(self):
        """Initialise the connector, making sure it is in a known state.
        After this function have been called, self.conn is an instance of the
        Connector the equipment have been configured to be connected to, and
        self.serport is an instance of the corresponding serialport (see
        pyserial).
        """
        # TODO: close and delete conn/port if exists
        # Check if the device is a USB/Serial device
        # If so, we detect which path the device is at
        # And reconfigure the attribute
        if (self.connector.startswith('/dev/ttyUSB')):
            res = subprocess.check_output(["ls", "/dev"]).split("\n")
            i = 0
            while True:
                if (res[i] != ''):
                    if res[i].startswith('ttyUSB'):
                        self.logger.debug("USB device has been found at /dev/%s" % res[i])
                        self.connector = "/dev/" + res[i]
                        break
                    else:
                        i = i + 1
                        continue
                else:
                    raise Errors.ENoAnswer("No USB device has been found")
        self.conn = connectors.get_connector(self.connector)
        self.serport = self.conn.get_serial_port()

        self.logger.info('Serial port:' + str(self.serport))

        return self.connector

    def doPre(self):
        self.resetProperties()
        self.initConnector()
        self.serport.setBaudrate(9600)
        self.conn.set_full_duplex(True)
        self.serport.open()
        self.serport.flushInput()
        self.conn.set_power(True)
        self.serport.setTimeout(10)

    def doPost(self):
        self.serport.flushInput()
        self.serport.close()
        self.conn.is_faulty()

    def doProbe(self):
        self.serport.setTimeout(6)
        time.sleep(2)
        lines = []
        eof = False

        self.serport.write("get all\r")

        limit = 0

        first = int(time.time())

        while not eof:

            if int(time.time()) - first > 30:
                self.logger.info('Get All Timed Out')
                return

            if limit >= 5:
                self.logger.debug('Failure with GET ALL command')
                return

            line = self.serport.readline()

            if len(line) == 0:
                limit += 1
                continue

            else:
                lines.append(line)

            if lines[-1].startswith('OUTPUTSER HEADER'):
                lines.append(self.serport.readline())
                eof = True

        for x in lines:
            self.logger.info('Line read from Get All : {0}'.format(x))

            """

            if x.startswith('VOLTAGE')          : self.voltage = re.findall(r"[-+]?\d*\.\d+|\d+", x)[0]
            if x.startswith('VERSION')          : self.software_version = re.findall(r"[-+]?\d*\.\d+|\d+", x)[0]
            if x.startswith('OUTPUTSER\t')      : self.outputser = x[-6:]
            if x.startswith('POLLING')          : self.polling = re.findall(r'\d+', x)[0]
            if x.startswith('RESETCOUNT')       : self.resetcount = re.findall(r'\d+', x)[0]
            if x.startswith('CALSTATUS')        : self.calstatus = re.findall(r'\d+', x)[0]
            if x.startswith('SUMMARYSER')       : self.summaryser = re.findall(r'\d+', x)[0]
            if x.startswith('OUTPUTSER HEADER') : self.ser_header = lines[-1]

            """

        # print(self.voltage, self.software_version, self.outputser, self.polling, self.resetcount, self.calstatus, self.summaryser, self.ser_header)

    def read_data(self):

        data = ''

        self.serport.write("poll\r")

        attempts = 0

        while attempts < 10:

            line = self.serport.readline()

            if not line:
                self.logger.debug('Not getting data from uWave')

            else:
                self.logger.debug('Read:' + line)

                if 'PSVSW' in line:
                    data = line

                    data = (data.replace('\x00', '')).rstrip('\r\n')
                    return data

            attempts += 1

        self.logger.debug('Error in read_data')
        return None

    def doSample(self):

        try:
            self.doProbe()
        except:
            self.logger.info('Probe Failed')

        self.serport.flushInput()

        self.logger.info("Waiting for uWave")

        self.data_string = self.read_data()

        if self.data_string is not None:

            try:
                nb_fields = self.data_string.split(',')
                nb_fields = len(nb_fields)

                self.logger.debug(self.data_string)
                self.logger.debug('nb_fields = ' + str(nb_fields))

                samples = utils.parse_nmea(self.data_string, nb_fields)

            except Exception as e:

                self.logger.info('Error in parse_nmea :' + str(e))

                samples = self.data_string.split(',')  # in case parse_nmea fails

            try:

                self.logger.debug('Samples: {0}'.format(samples))

                self.heading = samples[1]
                self.sig_wave = samples[2]
                self.dominant_periodFW = samples[3]
                self.avr_periodTz = samples[4]
                self.avr_periodTe = samples[5]
                self.avr_periodAPD = samples[6]
                self.dominant_waveDirFW = samples[7]
                self.mean_waveDir = samples[8]
                self.h_max = samples[9]
                self.h_max2 = samples[10]
                self.p_max = samples[11]
                self.a1 = samples[12]
                self.b1 = samples[13]
                self.a2 = samples[14]
                self.b2 = samples[15]
                # self.acc_n = samples[16]
                # self.acc_e = samples[17]
                # self.acc_u = samples[18]
                # self.acc_x = samples[19]
                # self.acc_y = samples[20]
                # self.acc_z = samples[21]
                self.roll = samples[22]
                self.pitch = samples[23]
                # self.mag_x = samples[24]
                # self.mag_y = samples[25]
                # self.mag_z = samples[26]
                # self.maxAccX = samples[27]
                # self.maxAccY = samples[28]
                # self.maxAccZ = samples[29]
                self.datetime = samples[34].replace(' ', 'T') + 'UTC'
                # self.index    = samples[32]

                self.hs_third = float(self.sig_wave) / 3.0
                self.hs_third = str(self.hs_third)

                self.N = 1026.0 / float(self.avr_periodTz)  # 1026 is 17 mins and 6 seconds in seconds
                self.N = str(self.N)

                m1 = math.pow(float(self.a1), 2) + math.pow(float(self.b1), 2)
                m1 = math.sqrt(m1)
                spread = math.sqrt(2 - (2 * m1))
                self.wdir_sd = 2 * math.pi * spread
                self.wdir_sd = str(self.wdir_sd)

                self.logger.info('sig wave : {0}'.format(self.sig_wave))
                self.logger.info('dominant_periodFW : {0}'.format(self.dominant_periodFW))
                self.logger.info('dominant_waveDirFW : {0}'.format(self.dominant_waveDirFW))
                self.logger.info('h_max : {0}'.format(self.h_max))
                self.logger.info('wdir_sd : {0}'.format(self.wdir_sd))
                self.logger.info('datetime : {0}'.format(self.datetime))
                self.logger.info('zero crossings : {0}'.format(self.N))

                self.technical_status = None

            except Exception as e:
                self.logger.info('Error indexing samples')
                self.logger.debug(str(e))


        else:

            self.logger.info('Read None from serport')

    def downloadConfig(self):
        self.technical_status = None

    def doUploadConfig(self):
        self.technical_status = None

    def expect_line(self, start, timeout):
        first = int(time.time())
        self.logger.debug("[E%d] %s" % (timeout, start))
        self.serport.setTimeout(timeout)
        while (int(time.time()) - first) <= timeout:
            l = self.read_line()
            if l.startswith(start):
                return l
        raise Errors.ENoAnswer("Expecting \"%s\"" % start)

class uWave2USB(handler.Handler):

    equipment_type = "uwave_2_usb"

    def __init__(self, deployment_id, deployment_name):
        super(uWave2USB, self).__init__(deployment_id, deployment_name)

        self.addProperty(20, "heading", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(21, "sig_wave", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(22, "dominant_periodFW", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(23, "dominant_waveDirFW", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(24, "h_max", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(25, "h_max2", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(26, "avr_periodTz", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(27, "avr_periodTe", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(28, "avr_periodAPD", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(29, "mean_waveDir", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(30, "p_max", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(31, "a1", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(32, "b1", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(33, "a2", float, handler.PROP_SENSOR, 0.0)
        self.addProperty(34, "b2", float, handler.PROP_SENSOR, 0.0)
        # self.addProperty(35, "acc_n"      , float, handler.PROP_SENSOR, 0)
        # self.addProperty(36, "acc_e"      , float, handler.PROP_SENSOR, 0)
        # self.addProperty(37, "acc_u"      , float, handler.PROP_SENSOR, 0)
        # self.addProperty(39, "acc_x"      , float, handler.PROP_SENSOR, 0)
        # self.addProperty(40, "acc_y"      , float, handler.PROP_SENSOR, 0)
        # self.addProperty(41, "acc_z"      , float, handler.PROP_SENSOR, 0)
        self.addProperty(42, "roll", float, handler.PROP_SENSOR, 0)
        self.addProperty(43, "pitch", float, handler.PROP_SENSOR, 0)
        # self.addProperty(44, "mag_x", float, handler.PROP_SENSOR, 0)
        # self.addProperty(45, "mag_y", float, handler.PROP_SENSOR, 0)
        # self.addProperty(46, "mag_z", float, handler.PROP_SENSOR, 0)
        # self.addProperty(47, "maxAccX"   , float, handler.PROP_SENSOR, 0)
        # self.addProperty(48, "maxAccY"   , float, handler.PROP_SENSOR, 0)
        # self.addProperty(49, "maxAccZ"   , float, handler.PROP_SENSOR, 0)
        self.addProperty(50, "datetime", str, handler.PROP_SENSOR, '0')  # contains date + time
        # self.addProperty(51, "index"      , int, handler.PROP_SENSOR, 0)
        self.addProperty(52, "wdir_sd", float, handler.PROP_SENSOR, 0)
        self.addProperty(53, "hs_third", float, handler.PROP_SENSOR, 0)
        self.addProperty(54, "N", float, handler.PROP_SENSOR, 0.0)

        """
        self.addProperty(55, "voltage"    , str, handler.PROP_STATUS, '0')
        self.addProperty(56, "outputser"  , str, handler.PROP_STATUS, '0')
        self.addProperty(57, "resetcount" , str, handler.PROP_STATUS, '0')
        self.addProperty(58, "cal_status" , str, handler.PROP_STATUS, '0')
        self.addProperty(59, "polling"    , str, handler.PROP_STATUS, '0')
        self.addProperty(60, "summaryser" , str, handler.PROP_STATUS, '0')
        self.addProperty(61, "ser_header" , str, handler.PROP_STATUS, '0')
        """

    def initConnector(self):
        pass

    def doPre(self):
        self.resetProperties()

    def readOldLine(self):

        try:
            file = '/tmp/waveLine.txt'

            if os.path.isfile(file):

                with open(file,'r') as f:

                    self.oldLine = f.read()

                    self.logger.info('old line : {0}'.format(self.oldLine))

            else:
                self.oldLine = False

        except Exception as e:
            self.logger.info('Critical error with old line reading: {0}'.format(e))
            self.oldLine = False

    def writeOldLine(self):

        try:
            file = '/tmp/waveLine.txt'

            with open(file,'w') as f:

                f.write(self.data_string)

                self.logger.info('data string written to {0} , data : {1}'.format(file,self.data_string))

        except Exception as e:
            self.logger.info('Critical error writing to {0} : {1}'.format(file,e))

    def doPost(self):
        pass

    def getFileName(self):

        folder = '/mnt/sdcard/waveData/'

        now = datetime.datetime.now()

        filename = now.strftime('%Y-%m-%d')
        filename = 'wave-' + filename + '.log'

        file = folder+'/'+filename

        if os.path.exists(file):
            return file

        else:
            return False

    def findLatestLine(self,file,search,timeout=60):

        if not os.path.exists(file):
            print('File not there')
            return False

        first = time.time()

        for line in self.reverse_lines(file):

            if search in line:
                print('Found line : {0}'.format(line))
                return line

            if time.time() - first >= timeout:
                print('No time left to read file')
                return False

        return False

    def reverse_lines(self, filename, BUFSIZE=4096):
        f = open(filename, "r")
        f.seek(0, 2)
        p = f.tell()
        remainder = ""
        while True:
            sz = min(BUFSIZE, p)
            p -= sz
            f.seek(p)
            buf = f.read(sz) + remainder
            if '\n' not in buf:
                remainder = buf
            else:
                i = buf.index('\n')
                for L in buf[i + 1:].split("\n")[::-1]:
                    yield L
                remainder = buf[:i]
            if p == 0:
                break
        yield remainder

    def doProbe(self):
        pass

    def read_data(self):

        file = self.getFileName()

        if file is not False:

            line = self.findLatestLine(file,'PSVSW')

            return line

        else:
            return False

    def doSample(self):

        self.readOldLine()

        self.data_string = self.read_data()

        if self.data_string is False:
            self.logger.into('No data available')
            return

        else:

            if self.oldLine is not False:

                self.logger.debug('data : {0} old : {1}'.format(self.data_string, self.oldLine))

                if self.data_string == self.oldLine:
                    self.logger.info('Duplication found, data : {0} old : {1}'.format(self.data_string,self.oldLine))
                    return

                else:
                    self.logger.info('Data string and old data are different')


        self.writeOldLine()

        try:
            nb_fields = self.data_string.split(',')
            nb_fields = len(nb_fields)

            self.logger.debug(self.data_string)
            self.logger.debug('nb_fields = ' + str(nb_fields))

            samples = utils.parse_nmea(self.data_string, nb_fields)

        except Exception as e:

            self.logger.info('Error in parse_nmea :' + str(e))

            try:
                samples = self.data_string.split(',')  # in case parse_nmea fails

            except Exception as e:
                self.logger.info('Bad error splitting data: {0}',e)

        try:

            self.logger.debug('Samples: {0}'.format(samples))

            self.heading = samples[1]
            self.sig_wave = samples[2]
            self.dominant_periodFW = samples[3]
            self.avr_periodTz = samples[4]
            self.avr_periodTe = samples[5]
            self.avr_periodAPD = samples[6]
            self.dominant_waveDirFW = samples[7]
            self.mean_waveDir = samples[8]
            self.h_max = samples[9]
            self.h_max2 = samples[10]
            self.p_max = samples[11]
            self.a1 = samples[12]
            self.b1 = samples[13]
            self.a2 = samples[14]
            self.b2 = samples[15]
            # self.acc_n = samples[16]
            # self.acc_e = samples[17]
            # self.acc_u = samples[18]
            # self.acc_x = samples[19]
            # self.acc_y = samples[20]
            # self.acc_z = samples[21]
            self.roll = samples[22]
            self.pitch = samples[23]
            # self.mag_x = samples[24]
            # self.mag_y = samples[25]
            # self.mag_z = samples[26]
            # self.maxAccX = samples[27]
            # self.maxAccY = samples[28]
            # self.maxAccZ = samples[29]
            self.datetime = samples[34].replace(' ', 'T') + 'UTC'
            # self.index    = samples[32]

            self.hs_third = float(self.sig_wave) / 3.0
            self.hs_third = str(self.hs_third)

            try:
                self.N = 1026.0 / float(self.avr_periodTz)  # 1026 is 17 mins and 6 seconds in seconds
                self.N = str(self.N)

            except Exception as e:
                self.logger.error(e)
                self.N = False

            m1 = math.pow(float(self.a1), 2) + math.pow(float(self.b1), 2)
            m1 = math.sqrt(m1)
            spread = math.sqrt(2 - (2 * m1))
            self.wdir_sd = 2 * math.pi * spread
            self.wdir_sd = str(self.wdir_sd)

            self.logger.info('sig wave : {0}'.format(self.sig_wave))
            self.logger.info('dominant_periodFW : {0}'.format(self.dominant_periodFW))
            self.logger.info('dominant_waveDirFW : {0}'.format(self.dominant_waveDirFW))
            self.logger.info('h_max : {0}'.format(self.h_max))
            self.logger.info('wdir_sd : {0}'.format(self.wdir_sd))
            self.logger.info('datetime : {0}'.format(self.datetime))
            self.logger.info('zero crossings : {0}'.format(self.N))

            self.technical_status = None

            return

        except Exception as e:
            self.logger.info('Error indexing samples, line is : {0}'.format(self.data_string))
            self.logger.info(str(e))

    def downloadConfig(self):
        pass

    def doUploadConfig(self):
        pass

    def expect_line(self, start, timeout):
        pass

