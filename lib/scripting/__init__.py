from zigzag import peak_valley_pivots, max_drawdown, compute_segment_returns, pivots_to_modes
import numpy as np
from lib.process import DataFrameWrapper

def handle_nan (v):
    if np.isnan(v):
        return None
    else:
        return float(v)

def transform (script, series, format, from_index, to_index):
    if "type" not in script:
        return dict(error="invalid script")

    if script["type"] == "pip":
        reader = DataFrameWrapper(series, from_index=from_index, to_index=to_index)
        return pip(script, reader)
    elif script["type"] == "sax":
        reader = DataFrameWrapper(series, from_index=from_index, to_index=to_index)
        return sax(script, reader)
    elif script["type"] == "search":
        return search(script, series, from_index, to_index)
    else:
        return dict(error="unknown script")

def get_pip (frame, column, error):
    out_frame = frame
    out_frame["direction"] = peak_valley_pivots(out_frame[column], error, -1*error)
    return out_frame[out_frame["direction"].isin([1,-1])]

def pip (script, reader):
    response = dict(series=[])
    frame = get_pip(reader.dframe, "open", 0.03)
    pips_series = { "id": 't:pips', "name": 'pips', "type": 'line', "data": [] }

    for index, row in frame.iterrows():
        pips_series["data"].append({"x": float(row["time"]/1000000)
                          , "y": handle_nan(row["open"]), "l": row["direction"]})
    response["series"].append(pips_series)

    return response


def sax_generate (script, frame):
    
    def znormalization(ts):
        """
        ts - each column of ts is a time series (np.ndarray)
        """
        mus = ts.mean(axis = 0)
        stds = ts.std(axis = 0)
        return (ts - mus) / stds

    def paa_transform(ts, n_pieces):
        """
        ts: the columns of which are time series represented by e.g. np.array
        n_pieces: M equally sized piecies into which the original ts is splitted
        """
        splitted = np.array_split(ts, n_pieces) ## along columns as we want
        return np.asarray(list(map(lambda xs: xs.mean(axis = 0), splitted)))

    def sax_transform(ts, n_pieces, alphabet):
        """
        ts: columns of which are time serieses represented by np.array
        n_pieces: number of segments in paa transformation
        alphabet: the letters to be translated to, e.g. "abcd", "ab"
        return np.array of ts's sax transformation
        Steps:
        1. znormalize
        2. ppa
        3. find norm distribution breakpoints by scipy.stats
        4. convert ppa transformation into strings
        """
        from scipy.stats import norm
        alphabet_sz = len(alphabet)
        thrholds = norm.ppf(np.linspace(1./alphabet_sz, 
                                    1-1./alphabet_sz, 
                                    alphabet_sz-1))
        def translate(ts_values):
            try:
                return np.asarray([(alphabet[0] if ts_value < thrholds[0]
                    else (alphabet[-1] if ts_value > thrholds[-1]
                        else alphabet[np.where(thrholds <= ts_value)[0][-1]+1]))
                            for ts_value in ts_values])
            except IndexError:
                 print(ts_values)
                 raise

        paa_ts = paa_transform(znormalization(ts), n_pieces)
        try:
            return np.apply_along_axis(translate, 0, paa_ts)
        except IndexError:
            print(ts)
            print(paa_ts)
            return None

    sample = 9

    if "method" in script and script["method"] == "pip":
        frame = get_pip(frame, "open", 0.03)
        sample = len(frame)
        if len(frame) < 3:
            return None
    
    ret = {}
    ret["open_adjust"] = frame["open"].mean(axis=0)
    ret["normalized_open"] = znormalization(frame["open"])
    ret["paa_open"] = paa_transform(ret["normalized_open"], sample)
    ret["std_open"] = frame["open"].std(axis=0)
    ret["paa_time"] = np.asarray(list(map(lambda x: x[0], np.array_split(frame["time"], sample))))
    ret["sax_open"] = sax_transform(frame["open"], sample, "abc")
    ret["paa_open"] = (ret["paa_open"] * ret["std_open"]) + ret["open_adjust"]     
    return ret


def sax (script, reader):
    response = dict(series=[])
    sax_elements = sax_generate(script, reader.dframe)
    sax_series = { "id": "t:sax", "name": "sax", "type": "line", "data": [] }
    for index in range(0, len(sax_elements["paa_time"])):
        sax_series["data"].append(dict(x=float(sax_elements["paa_time"][index]/1000000)
                          , y=handle_nan(sax_elements["paa_open"][index]), l=sax_elements["sax_open"][index]))
        #                  , handle_nan(row["direction"])])
    response["series"].append(sax_series)
    return response
    
def search (script, series, from_index, to_index):
    reader = DataFrameWrapper(series)
    sax_search = sax_generate(script, reader.dframe[from_index:to_index].dropna(subset=["open"]))
    sax_search_string = "".join(["%s" % val for val in sax_search["sax_open"]])
    window_size = to_index - from_index
    window_low = max(4, window_size - 5)
    window_high = max(4, window_size + 5)
    row_count = len(reader.dframe)
    windows = {}
    for index in range (0, row_count):
         for size in range(window_low, window_high):
             window_end = index + size
             if window_end < row_count:
                 sax_window = sax_generate(script, reader.dframe[index:window_end].dropna(subset=["open"]))
                 #if sax_window == None: print("SAX_WINDOW")
                 #if sax_window["sax_open"] == None:
                 #    print("SAX_OPEN %s->%s %s" % (index, window_end, row_count))
                 #    print(reader.dframe[index:window_end])
                 if sax_window != None and (sax_search_string == "".join(["%s" % val for val in sax_window["sax_open"]])):
                     windows[str(index)] = window_end
    result_ranges = []
    key_ints = list(windows.keys())
    for i in range(0, len(key_ints)):
        key_ints[i] = int(key_ints[i])
    for index in sorted(key_ints):
         index_str = str(index)
         if len(result_ranges) == 0:
             result_ranges.append([index, windows[index_str]])
         else:
             last = result_ranges[-1]
             if index <= last[0] or index <= last[1]:
                 result_ranges[-1][1] = windows[index_str]
             else:
                 result_ranges.append([index, windows[index_str]])

    response = dict(match=[]) 
    for result in result_ranges:
        data = []
        count = 0
        print(result)
        for index, row in reader.dframe[result[0]:result[1]].iterrows():
            #data.append(dict(x=float(row["time"]/1000000)
            #              , y=handle_nan(row["open"])))
            data.append({"x":     float(row["time"]/1000000)
                        ,"open":  handle_nan(row["open"])
                        ,"high":  handle_nan(row["high"])
                        ,"low":   handle_nan(row["low"])
                        ,"close": handle_nan(row["close"])
                        ,"index": count})
            count += 1 

        match = dict(from_index=result[0], to_index=result[1], data=data)
        response["match"].append(match)
    return response 


