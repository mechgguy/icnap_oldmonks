from __future__ import annotations

import os
import sys

sys.path.append(os.path.dirname(__file__))
import base64
import io
import os
import xml.etree.ElementTree as ET

import pandas as pd


class SinumerikTraceHandler:
    def __init__(self) -> None:
        self.meta_vars = self._get_valid_meta_vars()
        self.index_var = self._get_valid_index_vars()
        self.variable_names = {}

    def read_xml(self, path: str) -> pd.DataFrame:
        with open(path, "rb") as file:
            encoded_string = base64.b64encode(file.read())
        decoded = base64.b64decode(encoded_string)
        return self.read_decoded_xml(decoded, path)

    def read_decoded_xml(
        self, decoded: bytes, filename: str, **kwargs
    ) -> pd.DataFrame:
        root = ET.fromstring(
            decoded.decode("utf-8", "replace")
        )  # TODO obacht; hot fix 'replace'

        # create data frame
        df = self.loop_xml_data(root)
        # strip column names
        col_names = self.get_xml_headers(root, **kwargs)
        df = df.rename(columns=col_names)
        # forward fill nan values
        df = df.fillna(method="ffill")
        # set dtypes
        df = df.astype("float64")
        # get metadata from filename
        self.meta_vars["equipment"], self.meta_vars["meas_type_id"] = (
            self._infer_metadata_from_session_name(
                os.path.splitext(filename)[0]
            )
        )
        # get metadata from xml
        self.meta_vars["sessionName"], self.meta_vars["start_time"] = (
            self._infer_metadata_from_xml(root)
        )
        for k, v in self.meta_vars.items():
            if v is not None:
                df[k] = v
        return df

    def loop_xml_data(self, root: ET) -> pd.DataFrame:
        rec = []
        for neighbor in root.iter("rec"):
            rec.append(neighbor.attrib)
        return pd.DataFrame(rec)

    def get_xml_headers(self, root: ET, **kwargs) -> pd.DataFrame:
        col_names = {}
        for i, signal in enumerate(
            root.findall("./traceData/dataFrame/dataSignal")
        ):
            signal_id = signal.get("id")
            signal_name = signal.get("name")
            if ("strip_col_names" in kwargs) & (
                bool(kwargs.get("strip_col_names"))
            ):
                signal_name = (
                    signal.get("name").rsplit("/")[-1].lstrip("nckServoData")
                )
            col_names.update({signal_id: signal_name})
        return col_names

    def wide_to_long(
        self, df: pd.DataFrame, meta_vars: dict = None, index_var: dict = None
    ) -> pd.DataFrame:
        if meta_vars == None:
            meta_vars = self.meta_vars
        if index_var == None:
            index_var = self.index_var
        value_cols = [
            value
            for value in df.columns
            if value not in list(meta_vars.keys()) + list(index_var.values())
        ]
        id_cols = [
            value
            for value in df.columns
            if value in list(meta_vars.keys()) + list(index_var.values())
        ]
        df = df.melt(
            value_vars=value_cols, id_vars=id_cols, ignore_index=False
        )
        return df

    def long_to_wide(
        self,
        df: pd.DataFrame,
        meta_vars: dict = None,
        index_var: dict = None,
        preprocess_func: function = None,
        **kwargs,
    ) -> pd.DataFrame:
        if meta_vars == None:
            meta_vars = self.meta_vars
        if index_var == None:
            index_var = self.index_var
        dfs = []
        n_metas = len(meta_vars)
        all_combinations_df = df[meta_vars.keys()].value_counts().reset_index()
        for metas in all_combinations_df.iterrows():
            metas = metas[1]
            df_ = df[
                (df[meta_vars.keys()] == metas.values[:n_metas]).all(axis=1)
            ]
            df_ = df_.pivot(
                index=index_var.values(), columns="variable", values="value"
            )
            if preprocess_func is not None:
                df_ = preprocess_func(df_, **kwargs)
                df_ = df_.reset_index()
            # add meta data
            df_.loc[:, meta_vars.keys()] = metas.values[:n_metas]
            dfs.append(df_)
        df = pd.concat(dfs, ignore_index=False)
        # df = df.reset_index()
        return df

    def remove_zero_diffs(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """
        Calculate differences according to selected column and return a new dataframe without zero differences.
        """
        diffs = df[col].diff()
        df = df[diffs != 0]
        return df

    def _infer_metadata_from_session_name(self, session_name: str) -> dict:
        strings = session_name.replace(" ", "_").split("_")
        for string in strings:
            if string.startswith("4951"):
                equipment = string
            if not string[0].isdigit():
                meas_type_id = string
        return equipment, meas_type_id

    def _infer_metadata_from_xml(self, root) -> dict:
        sessionName = root.find("./traceCaptureSetup/sessionSettings").get(
            "sessionName"
        )
        start_time = root.find("./traceData/dataFrame/frameHeader").get(
            "startTime"
        )
        return sessionName, start_time

    def _get_valid_meta_vars(self) -> dict:
        meta_vars = ["sessionName", "start_time", "equipment", "meas_type_id"]
        return dict(zip(meta_vars, [None] * len(meta_vars)))

    def _get_valid_meta_vars_options(self) -> dict:
        meta_vars = ["sessionName", "start_time", "equipment", "meas_type_id"]
        eq_options = self._get_equipment_options()
        meas_options = self._get_meas_options()
        return {
            "equipment": {
                "options": [{"label": i, "value": i} for i in eq_options]
            },
            "meas_type_id": {
                "options": [{"label": i, "value": i} for i in meas_options]
            },
        }

    def _get_equipment_options(self) -> list:
        ids = self.db_handler.get_pd_table(
            table_name="Equipments", columns=["id"]
        )
        return ids.sort_values("id").astype(str).values.flatten().tolist()

    def _get_meas_options(self) -> list:
        ids = self.db_handler.get_pd_table(
            table_name="MeasTypes", columns=["id"]
        )
        return ids.sort_values("id").astype(str).values.flatten().tolist()

    def _get_valid_index_vars(self) -> dict:
        return {"time": "time"}


######################################
#
# UTILITIES
#
######################################


def parse_contents(contents: str, filename: str, **kwargs) -> pd.DataFrame:
    """
    Parse single xml file and return pandas data frame
    """
    if type(contents) == str:
        _, content_string = contents.split(",")
    elif type(contents) == bytes:
        content_string = contents

    decoded = base64.b64decode(content_string)

    try:
        if "csv" in filename:
            # Assume that the user uploaded a CSV file
            df = pd.read_csv(io.StringIO(decoded.decode("utf-8")))
        elif "xls" in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded))
        elif "xml" in filename:
            # Assume that the user uploaded an XML file (SINUMERIK Trace)
            handler = SinumerikTraceHandler()  # TODO besser an f übergeben
            df = handler.read_decoded_xml(decoded, filename, **kwargs)
    except Exception as e:
        print(e)

    return df


def extract_metadata(contents: list, filename: str) -> pd.DataFrame:
    df = parse_contents(contents, filename)
    equipment = df.equipment[0]
    meas_type_id = df.meas_type_id[0]
    start_time = df.start_time[0]
    return {
        "filename": filename,
        "equipment": equipment,
        "meas_type_id": meas_type_id,
        "start_time": start_time,
    }
