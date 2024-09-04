/*
****************************************************************************
*  Copyright (c) 2024,  Skyline Communications NV  All Rights Reserved.    *
****************************************************************************

By using this script, you expressly agree with the usage terms and
conditions set out below.
This script and all related materials are protected by copyrights and
other intellectual property rights that exclusively belong
to Skyline Communications.

A user license granted for this script is strictly for personal use only.
This script may not be used in any way by anyone without the prior
written consent of Skyline Communications. Any sublicensing of this
script is forbidden.

Any modifications to this script by the user are only allowed for
personal use and within the intended purpose of the script,
and will remain the sole responsibility of the user.
Skyline Communications will not be responsible for any damages or
malfunctions whatsoever of the script resulting from a modification
or adaptation by the user.

The content of this script is confidential information.
The user hereby agrees to keep this confidential information strictly
secret and confidential and not to disclose or reveal it, in whole
or in part, directly or indirectly to any person, entity, organization
or administration without the prior written consent of
Skyline Communications.

Any inquiries can be addressed to:

	Skyline Communications NV
	Ambachtenstraat 33
	B-8870 Izegem
	Belgium
	Tel.	: +32 51 31 35 69
	Fax.	: +32 51 31 01 29
	E-mail	: info@skyline.be
	Web		: www.skyline.be
	Contact	: Ben Vandenberghe

****************************************************************************
Revision History:

DATE		VERSION		AUTHOR			COMMENTS

02/09/2024	1.0.0.1		DPR, Skyline	Initial version
****************************************************************************
*/

namespace SLCGQIDSHealthCheckResultsFilter_1
{
	using System;
	using System.Collections.Generic;
	using System.Linq;

	using Skyline.DataMiner.Analytics.GenericInterface;
	using Skyline.DataMiner.Net.Messages;

	[GQIMetaData(Name = "Health_Check_Results_Filter")]
	public class HealthCheckResultsFilter : IGQIDataSource, IGQIInputArguments, IGQIOnInit
	{
		private readonly GQIDateTimeArgument _start = new GQIDateTimeArgument("Start Date") { IsRequired = true };
		private readonly GQIDateTimeArgument _end = new GQIDateTimeArgument("Start End") { IsRequired = true };

		private DateTime _startValue;
		private DateTime _endValue;

		private GQIDMS _dms;

		public GQIColumn[] GetColumns()
		{
			return new GQIColumn[]
			{
			new GQIStringColumn("Index"),
			new GQIStringColumn("Test"),
			new GQIStringColumn("Result"),
			new GQIDoubleColumn("Failure Rate % (Last run)"),
			new GQIDoubleColumn("Failure Rate % (Long Duration)"),
			new GQIDateTimeColumn("Time"),
			};
		}

		public GQIArgument[] GetInputArguments()
		{
			return new GQIArgument[] { _start, _end };
		}

		public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
		{
			_startValue = args.GetArgumentValue(_start).ToLocalTime();
			_endValue = args.GetArgumentValue(_end).ToLocalTime();
			return default;
		}

		public GQIPage GetNextPage(GetNextPageInputArgs args)
		{
			return GetData();
		}

		public OnInitOutputArgs OnInit(OnInitInputArgs args)
		{
			_dms = args.DMS;
			return default;
		}

		private static string CreateColumnFilter(params int[] columnPIDs)
		{
			return $"columns={string.Join(",", columnPIDs)}";
		}

		private GQIPage GetData()
		{
			var elements = GetElements().ToList();

			if (elements.Count != 1)
			{
				return new GQIPage(new List<GQIRow>().ToArray()) { HasNextPage = false };
			}

			var filters = new[] { "ForceFullTable=true", CreateColumnFilter(2002, 2003, 2004, 2006, 2007, 2008) };

			var resultTable = GetTablePage(elements[0].DataMinerID, elements[0].ElementID, 2000, filters)?.NewValue;

			if (resultTable == null)
			{
				return new GQIPage(new List<GQIRow>().ToArray()) { HasNextPage = false };
			}

			var columns = resultTable.ArrayValue;

			if (columns.Length != 7)
			{
				return new GQIPage(new List<GQIRow>().ToArray()) { HasNextPage = false };
			}

			Dictionary<string, Test> dicTests = GetLatestTests(columns);

			var rows = new List<GQIRow>();

			foreach (var testResult in dicTests)
			{
				double longRun = Convert.ToInt32(testResult.Value.Total) == 0 ? 0 : (testResult.Value.Failed / testResult.Value.Total) * 100;

				var cells = new List<GQICell>
				{
					new GQICell { Value = testResult.Value.Index},
					new GQICell { Value = testResult.Value.Name},
					new GQICell { Value = testResult.Value.Result },
					new GQICell { Value = testResult.Value.FailureRate, DisplayValue=$"{ Math.Round(testResult.Value.FailureRate, 2)} %"},
					new GQICell { Value = longRun, DisplayValue = $"{Math.Round(longRun, 2)} %" },
					new GQICell { Value = testResult.Value.Time.ToUniversalTime() },
				};

				var rowData = new GQIRow(cells.ToArray());
				rows.Add(rowData);
			}

			return new GQIPage(rows.ToArray()) { HasNextPage = false };
		}

		private Dictionary<string, Test> GetLatestTests(ParameterValue[] columns)
		{
			ParameterValue[] testIndex = columns[0].ArrayValue;
			ParameterValue[] testName = columns[1].ArrayValue;
			ParameterValue[] result = columns[2].ArrayValue;
			ParameterValue[] failureRate = columns[3].ArrayValue;
			ParameterValue[] resultDate = columns[4].ArrayValue;
			ParameterValue[] sucess = columns[5].ArrayValue;
			ParameterValue[] fail = columns[6].ArrayValue;

			Dictionary<string, Test> dicTests = new Dictionary<string, Test>();

			for (int i = 0; i < testName.Length; i++)
			{
				string testNameValue = testName[i].CellValue.StringValue;

				DateTime dt = DateTime.FromOADate(resultDate[i].CellValue.DoubleValue);

				if (dt < _startValue || dt > _endValue)
				{
					continue;
				}

				string testResult = Convert.ToInt32(result[i].CellValue.DoubleValue) == 0 ? "OK" : "Fail";
				string testIndexValue = testIndex[i].CellValue.StringValue;

				if (dicTests.ContainsKey(testNameValue))
				{
					if (dt > dicTests[testNameValue].Time)
					{
						dicTests[testNameValue].Result = testResult;
						dicTests[testNameValue].FailureRate = failureRate[i].CellValue.DoubleValue;
						dicTests[testNameValue].Time = dt;
						dicTests[testNameValue].Index = testIndexValue;
					}

					dicTests[testNameValue].Total += sucess[i].CellValue.DoubleValue + fail[i].CellValue.DoubleValue;
					dicTests[testNameValue].Failed += fail[i].CellValue.DoubleValue;
				}
				else
				{
					Test test = new Test();
					test.Index = testIndexValue;
					test.Name = testNameValue;
					test.Result = testResult;
					test.FailureRate = failureRate[i].CellValue.DoubleValue;
					test.Total = sucess[i].CellValue.DoubleValue + fail[i].CellValue.DoubleValue;
					test.Failed = fail[i].CellValue.DoubleValue;
					test.Time = dt;

					dicTests.Add(testNameValue, test);
				}
			}

			return dicTests;
		}

		private ParameterChangeEventMessage GetTablePage(int dmaID, int elementID, int parameterID, string[] filters)
		{
			var request = new GetPartialTableMessage(dmaID, elementID, parameterID, filters);
			try
			{
				return _dms.SendMessage(request) as ParameterChangeEventMessage;
			}
			catch (Exception)
			{
				return default;
			}
		}

		private IEnumerable<LiteElementInfoEvent> GetElements()
		{
			var request = new GetLiteElementInfo(includeStopped: false)
			{
				ProtocolName = "Skyline Health Check Manager",
				ProtocolVersion = "Production",
			};

			try
			{
				return _dms.SendMessages(request).OfType<LiteElementInfoEvent>();
			}
			catch (Exception)
			{
				return Array.Empty<LiteElementInfoEvent>();
			}
		}

		public class Test
		{
			public string Index { get; set; }

			public string Name { get; set; }

			public string Result { get; set; }

			public double FailureRate { get; set; }

			public string FailureRateLong { get; set; }

			public double Failed { get; set; }

			public double Total { get; set; }

			public DateTime Time { get; set; }
		}
	}
}
