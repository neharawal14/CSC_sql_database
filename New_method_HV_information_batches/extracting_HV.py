import pandas as pd
import cx_Oracle
from datetime import datetime, timedelta

            #AND CURRENT_CHANGE_DATE >= '06-AUG-22 11.12.05.761000000 PM'
class HVextraction:

    def __init__(self, year, chamber):
        self.year = year
        self.df = 'None'
        self.debug = True
        self.chamber = chamber
        try:
           self.con = cx_Oracle.connect("cms_csc_pvss_cond_r", "DQM_dcsread_pvss1", "cms_omds_adg")
           print(self.con.version)
           # Now execute the sqlquery
           # To get inside sqlserver
           self.cur = self.con.cursor()
        except cx_Oracle.DatabaseError as e:
           print("There is a problem with Oracle", e)
  
    def extracting_HV(self, df):
        self.df = df
        print(" going to extract HV")
        for index, row in self.df.iterrows():
            self.df.at[index, '_HV'] = self.calculate_HV(row['_dpid'],row['_timesecond'], row['_rhid'])

        #print(self.df['_HV','_rhid', '_dpid'])
        if self.con:
            self.cur.close()

        return self.df
        print(" finished extracting HV")
    def calculate_HV(self,dpid , time, rhid):
        if(self.debug):
            print(" dpid : timesecond : rhid ")
            print(dpid,"\t",  time ,"\t",  rhid, "\n")

        # My datetime object
        time_conv = datetime.utcfromtimestamp(time)
        formatted_time = time_conv.strftime('%d-%b-%y %I.%M.%S.000000000 %p').upper()
        one_day_before = time_conv - timedelta(days=1)
        one_day_after = time_conv + timedelta(days=2)
        # Format the dates and append '%' for use in SQL queries
        day_before = one_day_before.strftime('%d-%b-%y').upper()
        day_after = one_day_after.strftime('%d-%b-%y').upper()
        if(self.debug): 
            print("befor day :",day_before)
            print("after day :",day_after)

        if(self.debug):
            print("new formatted time ", formatted_time)

        if(self.chamber=="ME11a" or self.chamber =="ME11b"):
            query = """
			WITH ValidValues AS (
			SELECT
			DPID,
			ACTUAL_VMON AS CURRENT_HV,
			CHANGE_DATE AS CURRENT_CHANGE_DATE
			FROM
			CMS_CSC_PVSS_COND.FWCAENCHANNEL
			WHERE
            DPID = :1
            AND CHANGE_DATE BETWEEN TO_DATE(:2, 'DD-MON-YY') AND TO_DATE(:3, 'DD-MM-YY')
			AND ACTUAL_VMON IS NOT NULL
			),
			PreviousValues AS ( SELECT
			DPID,
			CURRENT_HV,
			CURRENT_CHANGE_DATE,
			LAG(CURRENT_HV) OVER (ORDER BY CURRENT_CHANGE_DATE) AS PREV_HV,
			LAG(CURRENT_CHANGE_DATE) OVER (ORDER BY CURRENT_CHANGE_DATE) AS CHANGE_DATE_PREV
			FROM
			ValidValues
			)
			SELECT DPID, CURRENT_HV, CURRENT_CHANGE_DATE,
			PREV_HV,
			CHANGE_DATE_PREV
			FROM
			PreviousValues
			WHERE
            CHANGE_DATE_PREV <= :4
            AND CURRENT_CHANGE_DATE > :4
			ORDER BY
			CURRENT_CHANGE_DATE
			FETCH FIRST ROW ONLY
            """
        else : 
            query = """
		    WITH PreviousValues AS (
		        SELECT
		            DPID,
		            VALUE AS CURRENT_HV,
		            CHANGE_DATE AS CURRENT_CHANGE_DATE,
		            LAG(VALUE) OVER (ORDER BY CHANGE_DATE) AS PREV_HV,
		            LAG(CHANGE_DATE) OVER (ORDER BY CHANGE_DATE) AS CHANGE_DATE_PREV
		        FROM CMS_CSC_PVSS_COND.CSC_HV_V_DATA
                WHERE DPID = :1
                AND CHANGE_DATE BETWEEN TO_DATE(:2,'DD-MON-YY') AND TO_DATE(:3,'DD-MM-YY')
                )
		    SELECT
		        DPID,
		        CURRENT_HV,
		        CURRENT_CHANGE_DATE,
		        PREV_HV,
		        CHANGE_DATE_PREV
		    FROM PreviousValues
		    WHERE
            CHANGE_DATE_PREV <= :4
            AND CURRENT_CHANGE_DATE > :4
            ORDER BY CURRENT_CHANGE_DATE
		    FETCH FIRST ROW ONLY
            """
        HV = 0
#        if(self.debug==True):
#            print(query)
        for row in self.cur.execute(query,[dpid, day_before, day_after, formatted_time]):
        #for row in self.cur.execute(query,[dpid]):
            if(self.debug):
                print(row)
                print("\n***")
                print(" row ", row[0])
                print(" row ", row[1])
                #print(" row1 ", row[1])
                #print(" row1 ", row[3])
            # row[0] is DPID, row[1] is Current HV, row[2] is current change date
            # row[3] is previous HV, row[4] previous change datae
            #HV = row[3]
            HV = row[3]
        return HV

