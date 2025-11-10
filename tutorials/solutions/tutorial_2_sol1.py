import datetime
from convert_crocolake_obs import ObsSequence

# define horizontal region
LAT0 = 5
LAT1 = 60
LON0 = -100
LON1 = -30

# define variables to import from CrocoLake
selected_variables = [
    "DB_NAME",  # ARGO, GLODAP, SprayGliders, OleanderXBT, Saildrones
    "JULD", # this contains timestamp
    "LATITUDE",
    "LONGITUDE",
    "PRES", # This will be automatically converted to depths in meters
    "TEMP",
    "PRES_QC",
    "TEMP_QC",
    "PRES_ERROR",
    "TEMP_ERROR",
    "PSAL",
    "PSAL_QC",
    "PSAL_ERROR"
]

# month and year are constant in out case
year0 = 2010
month0 = 5
wmo_id = str(1900256)
basename = basename + f".ARGO_{wmo_id}"
if not os.path.exists(outdir):
    os.makedirs(outdir, exist_ok=True)

# we loop to generate one file per day
for j in range(10):

    # set date range
    day0 = 1+j
    day1 = day0+1
    date0 = datetime.datetime(year0, month0, day0, 0, 0, 0)
    date1 = datetime.datetime(year0, month0, day1, 0, 0, 0)
    print(f"Converting obs between {date0} and {date1}")

    # this defines AND filters, i.e. we want to load each observation that has latitude within the given range AND longitude within the given range, etc.
    # to exclude NaNs, impose a range to a variable
    and_filters = (
        ("LATITUDE",'>',LAT0),  ("LATITUDE",'<',LAT1),
        ("LONGITUDE",'>',LON0), ("LONGITUDE",'<',LON1),
        ("PRES",'>',-1e30), ("PRES",'<',1e30),
        ("JULD",">",date0), ("JULD","<",date1),
        ("PLATFORM_NUMBER","==",wmo_id)
    )

    # this adds OR conditions to the and_filters, i.e. we want to load all observations that statisfy the AND conditions above, AND that have finite salinity OR temperature values
    db_filters = [
        list(and_filters) + [("PSAL", ">", -1e30), ("PSAL", "<", 1e30)],
        list(and_filters) + [("TEMP", ">", -1e30), ("TEMP", "<", 1e30)],
    ]

    # generate output filename
    obs_seq_out = basename + f".{year0}{month0:02d}{day0:02d}.out"

    # generate obs_seq.in file
    obsSeq = ObsSequence(
        crocolake_path,
        selected_variables,
        db_filters,
        obs_seq_out=obs_seq_out,
        loose=True
    )
    obsSeq.write_obs_seq()
