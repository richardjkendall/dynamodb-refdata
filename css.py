stylesheet = """
BODY {
        font-family: "Arial";
}

TABLE {
        border-width: 2;
        border-style: solid;
		border-collapse: collapse;
}

TR {
        border-width: 1;
        border-style: solid;
		border-collapse: collapse;
		background-color: #ffffff;
}

TR:nth-child(odd) {
	background-color: #efefef;
}

TH {
        border-width: 1;
        border-style: solid;
		border-collapse: collapse;
		background-color: #cfcfcf;
}

TD {
        border-width: 1;
        border-style: solid;
		text-align: center;
		border-collapse: collapse;
		padding: 5px;
}

.row_data {
	text-align: left;
}

.row_data p {
	text-decoration: underline;
}

.take_up_space {
        max-width: 99%;
}

.fixed_width {
        width: 120px
}

.TableTable {
        width: 100%
}

.ResultsTable {
	width: 80%;
}

.label {
        display: inline-block;
        font-weight: bold;
        font-size: 10pt;
        border-radius: 4px;
        padding-left: 4px;
        padding-right: 4px;
        padding-top: 2px;
        padding-bottom: 2px;
}

.create {
        background-color: #1E8E3E;
        color: #ffffff;
}

.update {
        background-color: #FA7B17;
        color: #ffffff;
}

.delete {
        background-color: #D93025;
        color: #ffffff;
}

.none {
        background-color: #efefef;
        color: #000000;
}
"""