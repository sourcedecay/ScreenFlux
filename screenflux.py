import os
import sqlite3
import datetime

import numpy
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot
import matplotlib.dates

knowledge_db = os.path.expanduser("~/Library/Application Support/Knowledge/knowledgeC.db")

def query_database():
    # Check if knowledgeC.db exists
    if not os.path.exists(knowledge_db):
        print("Could not find knowledgeC.db at %s." % (knowledge_db))
        exit(1)

    # Check if knowledgeC.db is readable
    if not os.access(knowledge_db, os.R_OK):
        print("The knowledgeC.db at %s is not readable.\nPlease grant full disk access to the application running the script (e.g. Terminal, iTerm, VSCode etc.)." % (knowledge_db))
        exit(1)

    # Connect to the SQLite database
    with sqlite3.connect(knowledge_db) as con:
        cur = con.cursor()
        
        # Execute the SQL query to fetch data
        # Modified from https://rud.is/b/2019/10/28/spelunking-macos-screentime-app-usage-with-r/
        query = """
        SELECT
            ZOBJECT.ZVALUESTRING AS "app", 
            (ZOBJECT.ZENDDATE - ZOBJECT.ZSTARTDATE) AS "usage",
            (ZOBJECT.ZSTARTDATE + 978307200) as "start_time", 
            (ZOBJECT.ZENDDATE + 978307200) as "end_time",
            (ZOBJECT.ZCREATIONDATE + 978307200) as "created_at", 
            ZOBJECT.ZSECONDSFROMGMT AS "tz",
            CASE ZSOURCE.ZDEVICEID
            WHEN ZSOURCE.ZDEVICEID THEN ZSOURCE.ZDEVICEID
            ELSE "Unknown"
            END "device_id",
            CASE ZMODEL
            WHEN ZMODEL THEN ZMODEL
            ELSE "Unknown"
            END "device_model"
        FROM
            ZOBJECT 
            LEFT JOIN
            ZSTRUCTUREDMETADATA 
            ON ZOBJECT.ZSTRUCTUREDMETADATA = ZSTRUCTUREDMETADATA.Z_PK 
            LEFT JOIN
            ZSOURCE 
            ON ZOBJECT.ZSOURCE = ZSOURCE.Z_PK 
            LEFT JOIN
            ZSYNCPEER
            ON ZSOURCE.ZDEVICEID = ZSYNCPEER.ZDEVICEID
        WHERE
            ZSTREAMNAME = "/app/usage"
        ORDER BY
            ZSTARTDATE DESC
        """
        cur.execute(query)
        
        # Fetch all rows from the result set
        return cur.fetchall()

def export_data(rows):
    # Export data to backup

    for dev in numpy.unique(rows["device_model"]):
        fp = os.path.expanduser("~/Nextcloud/coding/screentime/data_bkp")
        fn = "screentime_export_{}_{}.npy".format(
            dev, datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d%H%M%S")
        )
        numpy.save(os.path.join(fp, fn), rows)


def transform_data_arr(rows):
    data = []
    data_dtype = {
        "formats": ["O"] * 6,
        "names": [
            "app",
            "usage",
            "start_time",
            "end_time",
            "device_id",
            "device_model",
        ],
    }
    for r in rows:
        app = r[0]
        usage = r[1]
        st = r[2]
        et = r[3]
        device_id = r[6] or "Unknown"
        device_model = r[7] or "Unknown"

        data.append(
            tuple(
                [
                    app,
                    usage,
                    datetime.datetime.fromtimestamp(st),
                    datetime.datetime.fromtimestamp(et),
                    device_id,
                    device_model,
                ]
            )
        )

    return numpy.array(data, dtype=data_dtype)


def prepare_plot_data(data_rows, agg="month"):

    for mod in numpy.unique(data_rows["device_model"]):
        mod_arr = data_rows[data_rows["device_model"] == mod]
        if agg == "month":
            st_list = sorted(
                list(
                    set(
                        [
                            datetime.datetime(dt.year, dt.month, 1)
                            for dt in mod_arr["start_time"]
                        ]
                    )
                )
            )
            agg_list = []
            for st in st_list:
                if st.month == 12:
                    et = datetime.datetime(st.year + 1, 1, 1)
                else:
                    et = datetime.datetime(st.year, st.month + 1, 1)
                agg_list.append(tuple([st, et]))
            tdelta = datetime.timedelta(hours=8)
        elif agg == "day":
            st_list = sorted(
                list(
                    set(
                        [
                            datetime.datetime(dt.year, dt.month, dt.day)
                            for dt in mod_arr["start_time"]
                        ]
                    )
                )
            )
            agg_list = [(st, st + datetime.timedelta(days=1)) for st in st_list]
            tdelta = datetime.timedelta(hours=1)
        elif agg == "week":
            st_list = sorted(
                list(
                    set(
                        [
                            datetime.datetime(dt.year, dt.month, dt.day)
                            - datetime.timedelta(days=dt.weekday())
                            for dt in mod_arr["start_time"]
                        ]
                    )
                )
            )
            agg_list = [(st, st + datetime.timedelta(days=7)) for st in st_list]
            tdelta = datetime.timedelta(hours=12)
        elif agg == "year":
            st_list = sorted(
                list(
                    set(
                        [
                            datetime.datetime(dt.year, 1, 1)
                            for dt in mod_arr["start_time"]
                        ]
                    )
                )
            )
            agg_list = [(st, datetime.datetime(st.year + 1, 1, 1)) for st in st_list]
            tdelta = datetime.timedelta(days=5)
        else:
            print(
                "Use valid agg of 'day', 'week', 'month', or 'year'. Defaulting to 'year"
            )
            st_list = sorted(
                list(
                    set(
                        [
                            datetime.datetime(dt.year, dt.month, 1)
                            for dt in mod_arr["start_time"]
                        ]
                    )
                )
            )
            agg_list = [(st, datetime.datetime(st.year + 1, 1, 1)) for st in st_list]
            tdelta = datetime.timedelta(days=5)
        plot_dict = aggregate_data(mod_arr)
        for dt in agg_list:
            fp = os.path.expanduser("~/Nextcloud/coding/screentime/plots")
            fn = "{mod}_{agg}_{st}.png".format(mod=mod, agg=agg, st=dt[0].date())
            plot_data(os.path.join(fp, fn), plot_dict, dt[0], dt[1], tdelta)

    return


def plot_data(fname, plot_dict, start_time, end_time, tdelta):

    dict_names = list(plot_dict.keys())
    pos = [0.5 + idx for idx in range(len(dict_names))]
    st = matplotlib.dates.date2num(start_time)
    et = matplotlib.dates.date2num(end_time)
    fig, axes = matplotlib.pyplot.subplots()
    matplotlib.pyplot.xlim([start_time - tdelta, end_time + tdelta])
    matplotlib.pyplot.barh(
        pos, width=[0] * len(pos), left=[0] * len(pos), align="center"
    )
    for idx, p in enumerate(pos):
        filt = plot_dict[dict_names[idx]][
            (plot_dict[dict_names[idx]]["st"] <= et)
            & (plot_dict[dict_names[idx]]["et"] > st)
        ]
        ax = matplotlib.pyplot.barh(
            [p] * len(filt["st"]),
            width=list(filt["et"] - filt["st"]),
            left=list(filt["st"]),
            height=0.5,
            align="center",
        )
    matplotlib.pyplot.yticks(pos, dict_names)
    matplotlib.pyplot.subplots_adjust(left=0.05, wspace=0, hspace=0)
    matplotlib.pyplot.savefig(fname, bbox_inches="tight")
    matplotlib.pyplot.close()


def aggregate_data(rows):
    # Aggregate the screen time data into dictionary for bar graph over time

    plot_dict = {}
    plot_dtype = {"formats": ["O"] * 2, "names": ["st", "et"]}
    for app in numpy.unique(rows["app"]):
        plot_arr = []
        app_arr = rows[rows["app"] == app]
        for ent in app_arr:
            plot_arr.append(
                tuple(
                    [
                        matplotlib.dates.date2num(ent["start_time"]),
                        matplotlib.dates.date2num(ent["end_time"]),
                    ]
                )
            )
        plot_dict[app] = numpy.array(plot_arr, dtype=plot_dtype)

    return plot_dict


def main():
    # Query the database and fetch the row
    rows = query_database()

    # Do some basic clean up on None values and transform to datetime
    data_rows = transform_data_arr(rows)

    # Save data
    # export_data(data_rows)

    # Plot data
    prepare_plot_data(data_rows, agg="week")


if __name__ == "__main__":
    main()