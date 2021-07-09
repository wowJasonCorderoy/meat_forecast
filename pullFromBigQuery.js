'use strict';

function bq_pull() {
  SpreadsheetApp.getActiveSpreadsheet()
   .toast("Pulling changes from bigquery to this gsheet", "Hello", 200);

  // Replace this value with the project ID listed in the Google
  // Cloud Platform project.
  var projectId = 'gcp-wow-finance-de-lab-dev';

  var sql_string = 'SELECT * FROM `gcp-wow-finance-de-lab-dev.006_greenstockForecastModel.test_toReturnToGsheet`';

  var request = {
    query: sql_string,
    useLegacySql: false
  };
  var queryResults = BigQuery.Jobs.query(request, projectId);
  var jobId = queryResults.jobReference.jobId;

  // Check on status of the Query Job.
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }

  // Get all the rows of results.
  var rows = queryResults.rows;
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      pageToken: queryResults.pageToken
    });
    rows = rows.concat(queryResults.rows);
  }

  if (rows) {
    var spreadsheet = SpreadsheetApp.getActiveSpreadsheet()
    // var sheet = spreadsheet.getActiveSheet();
    var sheet = spreadsheet.getSheetByName("bqPull_Output");
    sheet.clear()

    // Append the headers.
    var headers = queryResults.schema.fields.map(function(field) {
      return field.name;
    });
    sheet.appendRow(headers);

    // Append the results.
    var data = new Array(rows.length);
    for (var i = 0; i < rows.length; i++) {
      var cols = rows[i].f;
      data[i] = new Array(cols.length);
      for (var j = 0; j < cols.length; j++) {
        data[i][j] = cols[j].v;
      }
    }
    sheet.getRange(2, 1, rows.length, headers.length).setValues(data);

    Logger.log('Results spreadsheet created: %s',
        spreadsheet.getUrl());
  } else {
    Logger.log('No rows returned.');
  }

  SpreadsheetApp.getActiveSpreadsheet()
   .toast("Changes pulled from bigquery", "Chow", 2);

}




