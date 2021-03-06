import os
import csv

PROJECT_PATH = '/home/crypdick/Apps/masters/dbs/ds-homelessness-FL'
RAW_DATA_PATH = os.path.join(PROJECT_PATH, 'data', 'raw')
os.chdir(PROJECT_PATH)
CSV_fnames = sorted([fname for fname in os.listdir(RAW_DATA_PATH) if fname.endswith('.csv')])
CSV_full_dirs = [os.path.join(RAW_DATA_PATH, fname) for fname in CSV_fnames]

MIN_ROW_LEN = 35
N_LEADING_COLS = 8  # all entries have this many leading cols
N_TRAILING_COLS = -14  # 13 trailing cols
with open('my_bookings_cleaned.csv', 'w') as my_bookings_cleaned_csv:
    with open('my_addl_charges.csv', 'w') as my_addl_charges_csv:
        writer = csv.writer(my_bookings_cleaned_csv)
        addl_writer = csv.writer(my_addl_charges_csv)

        for csv_file in CSV_full_dirs:
            with open(csv_file, 'r', newline='') as open_file:
                reader = csv.reader(open_file, delimiter=',', quotechar='"')
                print("starting ", csv_file)

                joined_row = []
                for row in reader:
                    if 'RELEASE DATE' not in row[0]:
                        joined_row.extend(row)  # have not reached end of person entry. continue extending joined row.
                    else:  # we found last row for single person
                        joined_row.extend(row)

                        if len(joined_row) < MIN_ROW_LEN:
                            raise Exception("Not expecting rows with less than {} columns".format(MIN_ROW_LEN))

                        # clean out random trailing spaces, replace commas inside of strings with periods
                        joined_row = [field.strip() for field in joined_row]
                        joined_row[1:] = [field.replace(",", ".") for field in joined_row[1:]]  # exclude name field

                        ####### joined row parsing #######
                        # parse lead
                        name = joined_row[0]
                        booking_n = joined_row[1]
                        agency = joined_row[4]
                        ABN = joined_row[5]
                        race_sex_eth_DOB = joined_row[7]
                        try:
                            race, sex, ethnicity, DOB = race_sex_eth_DOB.split(" / ")
                        except ValueError:  # some people missing DOB
                            # print("not enough values to unpack in str {} \n from joined row {}".format(race_sex_eth_DOB, joined_row))
                            race, sex, ethnicity = race_sex_eth_DOB.split(" / ")
                            DOB = "NaT"

                        # splitting crimes
                        crimes_vec = joined_row[N_LEADING_COLS:N_TRAILING_COLS]
                        full_crimes_list = []
                        while len(crimes_vec) >= 1:
                            single_crime = crimes_vec[:4]  # each loop grab first 4 cols
                            # make sure crime vec isn't empty string. sometimes first one is legitimately empty
                            if single_crime[0].strip() != '' or single_crime[1].strip() != '' :
                                full_crimes_list.append(single_crime)
                            crimes_vec = crimes_vec[7:]  # indexes 4:7 are always blank. beyond 7 are additional crimes.

                        ######## parse end #######
                        trailing_vec = joined_row[N_TRAILING_COLS:]
                        address = trailing_vec[0].replace("ADDRESS:","")  # combine two cols
                        place_of_birth = "{}{}".format(trailing_vec[1], trailing_vec[2]).replace("POB:","")
                        releaseDate = trailing_vec[7].replace("RELEASE DATE:","")
                        releaseCode = trailing_vec[8].replace("RELEASE CODE:","")
                        SOID = trailing_vec[9].replace("SOID:","")


                        # avoid index errors if full_crimes_list is empty
                        if not full_crimes_list:  # returns False if empty
                            full_crimes_list.append(['', '', '', ''])  # insert placeholders

                        ######## storing #######
                        clean_booking_vec = [name, booking_n, agency, ABN, race, sex, ethnicity, DOB] + \
                                            full_crimes_list[0] + [address, place_of_birth, releaseDate, releaseCode,
                                                                   SOID]
                        clean_booking_vec = [field.strip() for field in clean_booking_vec]
                        writer.writerow(clean_booking_vec)

                        # if addl crimes, store them in separate file
                        if len(full_crimes_list) > 1:
                            addl_charges = full_crimes_list[1:]
                            for charge in addl_charges:
                                clean_addl_booking = [booking_n] + charge  # Dr gillman's clean has booking# first col
                                addl_writer.writerow(clean_addl_booking)

                        # clear for next booking
                        joined_row = []