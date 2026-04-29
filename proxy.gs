const SHEET_ID = '1pd_vlN7azfbwGcZUauwUk5d_378mIlQHTWZN_PtntIU';

function doGet(e) {
  const sheet = e.parameter.sheet;
  if (!sheet) {
    return ContentService
      .createTextOutput(JSON.stringify({ error: 'missing sheet param' }))
      .setMimeType(ContentService.MimeType.JSON);
  }

  const url = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=${encodeURIComponent(sheet)}`;
  const response = UrlFetchApp.fetch(url);
  const text = response.getContentText();

  return ContentService
    .createTextOutput(text)
    .setMimeType(ContentService.MimeType.TEXT);
}
