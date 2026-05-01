const SHEET_ID = '1pd_vlN7azfbwGcZUauwUk5d_378mIlQHTWZN_PtntIU';
const ADMIN_PASSWORD_PROPERTY = 'ADMIN_PASSWORD';
const TOKEN_TTL_SECONDS = 6 * 60 * 60;

const SUBJECTS = [
  { key: 'математика', label: 'Математика', levels: ['ЕГЭ', 'ОГЭ'], short: 'мат' },
  { key: 'информатика', label: 'Информатика', levels: ['ЕГЭ', 'ОГЭ'], short: 'инф' },
  { key: 'русский язык', label: 'Русский язык', levels: ['ЕГЭ', 'ОГЭ'], short: 'ря' },
  { key: 'обществознание', label: 'Обществознание', levels: ['ЕГЭ', 'ОГЭ'], short: 'общ' },
  { key: 'история', label: 'История', levels: ['ЕГЭ'], short: 'ист' },
  { key: 'физика', label: 'Физика', levels: ['ЕГЭ', 'ОГЭ'], short: 'физ' },
];

function doGet(e) {
  const params = e.parameter || {};
  const mode = params.mode || 'public';

  try {
    if (mode === 'login') {
      return handleLogin(params);
    }

    if (mode === 'admin') {
      return handleAdmin(params);
    }

    return handlePublic(params);
  } catch (error) {
    return jsonResponse({ ok: false, error: error.message });
  }
}

function handleLogin(params) {
  const password = params.password || '';
  const adminPassword = PropertiesService.getScriptProperties().getProperty(ADMIN_PASSWORD_PROPERTY);

  if (!adminPassword) {
    throw new Error(`Script property ${ADMIN_PASSWORD_PROPERTY} is not configured`);
  }

  if (password !== adminPassword) {
    return jsonResponse({ ok: false, error: 'unauthorized' });
  }

  const token = Utilities.getUuid();
  CacheService.getScriptCache().put(getTokenKey(token), '1', TOKEN_TTL_SECONDS);
  return jsonResponse({ ok: true, token, expiresIn: TOKEN_TTL_SECONDS });
}

function handleAdmin(params) {
  requireToken(params.token);

  const rows = [];
  for (const subject of SUBJECTS) {
    for (const level of subject.levels) {
      const sheetName = getSheetName(subject.key, level);
      const gviz = fetchSheetGviz(sheetName);
      rows.push(...parseRows(gviz, subject.key, level, true));
    }
  }

  return jsonResponse({ ok: true, rows });
}

function handlePublic(params) {
  const sheet = params.sheet;
  if (!sheet) {
    throw new Error('missing sheet param');
  }

  const subjectLevel = findSubjectLevelBySheet(sheet);
  if (!subjectLevel) {
    throw new Error('unknown sheet');
  }

  const gviz = fetchSheetGviz(sheet);
  const rows = parseRows(gviz, subjectLevel.subject, subjectLevel.level, false);
  return jsonResponse({ ok: true, rows });
}

function requireToken(token) {
  if (!token) {
    throw new Error('missing token');
  }

  const valid = CacheService.getScriptCache().get(getTokenKey(token)) === '1';
  if (!valid) {
    throw new Error('unauthorized');
  }
}

function getTokenKey(token) {
  return `admin_${token}`;
}

function getSheetName(subject, level) {
  const item = SUBJECTS.find((subjectItem) => subjectItem.key === subject);
  if (!item) {
    throw new Error(`unknown subject: ${subject}`);
  }
  return `${item.short} ${level}`;
}

function findSubjectLevelBySheet(sheetName) {
  for (const subject of SUBJECTS) {
    for (const level of subject.levels) {
      if (getSheetName(subject.key, level) === sheetName) {
        return { subject: subject.key, level };
      }
    }
  }
  return null;
}

function fetchSheetGviz(sheetName) {
  const url = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:json&sheet=${encodeURIComponent(sheetName)}`;
  const response = UrlFetchApp.fetch(url);
  const text = response.getContentText();
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}') + 1;

  if (start === -1 || end <= start) {
    throw new Error(`failed to parse sheet: ${sheetName}`);
  }

  return JSON.parse(text.substring(start, end));
}

function parseRows(gvizJson, subject, level, includeDetails) {
  const table = gvizJson.table;
  if (!table || !table.rows) {
    throw new Error('unexpected sheet format');
  }

  const result = [];
  const headerCells = (table.rows[0] && table.rows[0].c) || [];
  const dailyColumns = includeDetails ? getDailyColumns(headerCells) : [];

  for (let rowIdx = 1; rowIdx < table.rows.length; rowIdx++) {
    const cells = table.rows[rowIdx].c || [];
    const name = cells[2] && cells[2].v;

    if (!name || String(name).trim() === '') {
      continue;
    }

    const row = {
      subject,
      level,
      name: String(name).trim(),
      teacher: getCellString(cells[13]) || 'Без преподавателя',
      score: getCellNumber(cells[10]),
    };

    if (includeDetails) {
      row.group = getCellString(cells[1]);
      row.daysDone = getCellNumber(cells[3]);
      row.daysTotal = getCellNumber(cells[4]);
      row.coefficient = getCellNumber(cells[5]);
      row.quality = getCellNumber(cells[6]);
      row.baseScore = getCellNumber(cells[8]);
      row.penalty = getCellNumber(cells[9]);
      row.finalScore = getCellNumber(cells[10]);
      row.groupPlace = getCellNumber(cells[11]);
      row.schoolPlace = getCellNumber(cells[12]);
      row.dailyScores = dailyColumns
        .filter((column) => hasCellValue(cells[column.index]))
        .map((column) => ({
          dateKey: column.dateKey,
          dateLabel: column.dateLabel,
          dateOrder: column.dateOrder,
          score: getCellNumber(cells[column.index]),
        }));
    }

    result.push(row);
  }

  return result;
}

function getDailyColumns(headerCells) {
  const columns = [];

  for (let index = 14; index < headerCells.length; index++) {
    const cell = headerCells[index];
    const label = getCellDisplayString(cell).replace(/\.$/, '');

    if (!label) {
      continue;
    }

    if (/^родитель/i.test(label)) {
      break;
    }

    if (!/^\d{1,2}\./.test(label)) {
      continue;
    }

    columns.push({
      index,
      dateKey: getDateKey(cell, index),
      dateLabel: label,
      dateOrder: index,
    });
  }

  return columns;
}

function getDateKey(cell, fallbackIndex) {
  const value = cell && cell.v;

  if (typeof value === 'number') {
    return String(value);
  }

  if (typeof value === 'string') {
    const match = value.match(/^Date\((\d+),(\d+),(\d+)\)$/);
    if (match) {
      const year = match[1];
      const month = String(Number(match[2]) + 1).padStart(2, '0');
      const day = String(Number(match[3])).padStart(2, '0');
      return `${year}-${month}-${day}`;
    }
  }

  return `column-${fallbackIndex}`;
}

function getCellString(cell) {
  const value = cell && (cell.v !== null && cell.v !== undefined ? cell.v : cell.f);
  return value === null || value === undefined ? '' : String(value).trim();
}

function getCellDisplayString(cell) {
  const value = cell && (cell.f !== null && cell.f !== undefined ? cell.f : cell.v);
  return value === null || value === undefined ? '' : String(value).trim();
}

function getCellNumber(cell) {
  const value = cell && (cell.v !== null && cell.v !== undefined ? cell.v : cell.f);
  if (value === null || value === undefined || value === '') {
    return 0;
  }
  return Number(String(value).replace(',', '.')) || 0;
}

function hasCellValue(cell) {
  const value = cell && (cell.v !== null && cell.v !== undefined ? cell.v : cell.f);
  return value !== null && value !== undefined && value !== '';
}

function jsonResponse(payload) {
  return ContentService
    .createTextOutput(JSON.stringify(payload))
    .setMimeType(ContentService.MimeType.JSON);
}
