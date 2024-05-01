import cx_Oracle
import pandas as pd
class DpidUpdate :
   
    def __init__(self, year):
        self.year = year
        self.debug = False
        print("initialise SQL")
        try:
          self.con = cx_Oracle.connect("cms_csc_pvss_cond_r", "DQM_dcsread_pvss1", "cms_omds_adg")
          
          print(self.con.version)
         
          # Now execute the sqlquery
          self.cur = self.con.cursor()
        except cx_Oracle.DatabaseError as e:
          print("There is a problem with Oracle", e)
        
       # by writing finally if any error occurs
       # then also we can close the all database operation

    def extract_dpid(self, df):
        print("initialise extracting dpid ")
        dpid_values = [self.calculate_dpid(row['_rhid']) for index, row in df.iterrows()]
        df['_dpid'] = dpid_values

        print(" extracted dpid ")
        if self.cur:
            self.cur.close()
        if self.con:
            self.con.close()


        return df
    def calculate_dpid(self,rhid):
        endcap = rhid // 1000000
        if(endcap==1) :
            endcap_string='plus'
            endcap_sign = '+'
            endcap_symbol = "P"
        if(endcap==2) :
            endcap_string='minus'
            endcap_sign = '-'
            endcap_symbol = "M"
        rhid %= 1000000
        station = rhid // 100000
        rhid %= 100000
        ring = rhid // 10000
        if(ring==4) :
            ring=1

        rhid %= 10000
        chamber_number = rhid // 100
        rhid %= 100
        layer = rhid // 10
        rhid %= 10
        hv_channel = rhid
        if((station == 1 and ring ==1) and (self.year=="2016" or self.year=="2017" or self.year=="2018")) :
            input_file = "/afs/cern.ch/work/n/nrawal/sql_access/DPID_project/ME11_HV_Mapping_CAEN.csv"
            string_chamber = "ME"+endcap_sign+str(station)+"/"+str(ring)+"/"+(str(chamber_number)).zfill(2)+"/"+str(layer)
            if(self.debug) :
                print("to find caen mapping :",string_chamber)
            f = open(input_file,"r")
            lines = f.readlines()[1:]
            for line in lines:
                strip_line = line.strip();
                split_line = line.split(";")
                channel_id = split_line[1]
                channel_id = channel_id.strip()
                #print(" channel id :",channel_id)
                if(string_chamber==channel_id):
                    dpid_name = split_line[0]
                    if(self.debug):
                        print("chamber name :",string_chamber, "\n dpid name :", dpid_name) 
                    break
        else:
            if(endcap_string=='minus') :
                machine_name = "cms_csc_dcs_2"
            if(endcap_string=='plus'):
                machine_name = "cms_csc_dcs_3"
            if ( (station==2 and ring==2) or (station==3 and ring==2) or (station==4 and ring==2))  :
                if(layer<=3) : 
                    channel_nb = layer + 3 *(hv_channel-1)
                if(layer >=4):
                    channel_nb = 15+ (layer-3) + 3 *(hv_channel-1)
            else : 
                channel_nb = layer + 6 *(hv_channel-1)

            dpid_name = machine_name+":CSC_ME_"+endcap_symbol+str(station)+str(ring)+"_C"+(str(chamber_number)).zfill(2)+"_HV_V"+(str(channel_nb)).zfill(2)+"_VMON"
            #if(self.debug):
                #print("chamber name :",dpid_name) 

        query = "SELECT ID, DPNAME FROM CMS_CSC_PVSS_COND.DP_NAME2ID WHERE DPNAME='"+dpid_name+"'"
        if(self.debug):
            print(query)
        for row in self.cur.execute(query):
            if(self.debug):
                print(row)
                print(row[0],"name : ",row [1])
            dpid = row[0]
        if(self.debug):
           print(" dpid ", dpid)
        return dpid
