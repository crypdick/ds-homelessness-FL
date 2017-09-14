import os
import csv

PROJECT_PATH = '/home/crypdick/Apps/masters/dbs/ds-homelessness-FL'
RAW_DATA_PATH = os.path.join(PROJECT_PATH, 'data', 'raw')
os.chdir(PROJECT_PATH)

# CSV_fnames = sorted(os.listdir(os.path.join(os.path.realpath('.'), RAW_DATA_PATH)))
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
                for i, row in enumerate(reader):
                    if 'RELEASE DATE' not in row[0]:
                        joined_row.extend(row)  # have not reached end of person entry. continue extending joined row.
                    else:  # we found last row for single person
                        joined_row.extend(row)
                        # print(joined_row)

                        if len(joined_row) < MIN_ROW_LEN:
                            raise Exception("Not expecting rows with less than {} columns".format(MIN_ROW_LEN))

                        # clean out random trailing spaces, replace commas inside of strings with periods
                        joined_row = [field.strip().replace(",", ".") for field in joined_row]

                        # joined row parsing

                        # parse lead
                        name = joined_row[0]
                        booking_n = joined_row[1]
                        agency = joined_row[4]
                        ABN = joined_row[5]
                        race_sex_eth_DOB = joined_row[7]
                        try:
                            race, sex, ethnicity, DOB = race_sex_eth_DOB.split(" / ")
                        except ValueError:  # some people missing DOB
                            race, sex, ethnicity = race_sex_eth_DOB.split(" / ")
                            DOB = "NaT"
                            # print("not enough values to unpack in str {} \n from joined row {}".format(race_sex_eth_DOB, joined_row))

                        # splitting crimes
                        crimes_vec = joined_row[N_LEADING_COLS:N_TRAILING_COLS]
                        full_crimes_list = []
                        while len(crimes_vec) >= 1:
                            single_crime = crimes_vec[:4]  # each loop grab first 4 cols
                            if single_crime[0].strip() != '':  # make sure crime vec isn't empty string
                                full_crimes_list.append(single_crime)
                            crimes_vec = crimes_vec[7:]  # indexes 4:7 are always blank. beyond 7 are additional crimes.

                        # parse end
                        trailing_vec = joined_row[N_TRAILING_COLS:]
                        address = "{}.{}".format(trailing_vec[0], trailing_vec[1]).replace("ADDRESS: ","")  # combine two cols
                        # address = address[9:]  # strip "ADDRESS: "
                        place_of_birth = trailing_vec[2].replace("POB: ","")
                        # place_of_birth = place_of_birth[5:]  # strip "POB: "
                        releaseDate = trailing_vec[7].replace("RELEASE DATE: ","")
                        # releaseDate = releaseDate[14:]  # strip "RELEASE DATE: "
                        releaseCode = trailing_vec[8].replace("RELEASE CODE: ","")
                        # releaseCode = releaseCode[14:]  # strip "RELEASE CODE: "
                        SOID = trailing_vec[9].replace("SOID: ","")
                        # SOID = SOID[6:]  # strip "SOID: "
                        # print(address,place_of_birth,releaseDate,releaseCode,SOID)


                        # avoid index errors if full_crimes_list is empty
                        if not full_crimes_list:  # returns False if empty
                            full_crimes_list.append(['', '', '', ''])  # insert placeholders

                        # storing
                        clean_booking_vec = [name, booking_n, agency, ABN, race, sex, ethnicity, DOB] + \
                                            full_crimes_list[0] + [address, place_of_birth, releaseDate, releaseCode,
                                                                   SOID]

                        # print(clean_booking_vec)
                        writer.writerow(clean_booking_vec)

                        # if addl crimes, store them in separate file
                        if len(full_crimes_list) > 1:
                            addl_charges = full_crimes_list[1:]
                            for charge in addl_charges:
                                clean_addl_booking = [booking_n] + charge
                                addl_writer.writerow(clean_addl_booking)

                        # clear joined row for next person
                        joined_row = []