/**
 * MENA Box Office Intelligence Workbook
 * Production-style Google Apps Script for acquisition research
 *
 * Sources implemented:
 *   - Box Office Mojo latest international weekends page (market-level chart toplines)
 *   - Box Office Mojo country/year weekend pages (market-level weekend chart toplines)
 *   - elCinema Egypt box office chart page (title-level current weekly chart)
 *   - elCinema title box office pages (title-level weekly history by country when available)
 *
 * Design rules enforced in code:
 *   - chart mentions are NOT treated as title performance
 *   - yearly / weekly / weekend / cumulative records are kept distinct
 *   - only comparable record types are reconciled together
 *   - ticket estimates are optional and clearly marked approximate
 *   - uncertain title matching goes to review queue
 */

// ============================================================
// SECTION 1: CONFIG
// ============================================================

const CONFIG = {
  VERSION: '4.1.0',
  TIMEZONE: Session.getScriptTimeZone() || 'Asia/Beirut',
  USER_AGENT: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',

  SHEETS: {
    LOOKUP: 'Acquisition_Lookup',
    DASHBOARD: 'Performance_Dashboard',
    RAW: 'Raw_Evidence',
    RECON: 'Reconciled_Evidence',
    FILMS: 'Film_Master',
    ALIASES: 'Film_Aliases',
    REVIEW: 'Review_Queue',
    RUN_LOG: 'Run_Log',
    TICKET_PRICES: 'Ticket_Prices',
    SOURCE_STATUS: 'Source_Status',
    CONFIG: 'System_Config'
  },

  MARKETS: {
    AE: { country: 'United Arab Emirates', area: 'AE', currency: 'AED' },
    SA: { country: 'Saudi Arabia', area: 'SA', currency: 'SAR' },
    EG: { country: 'Egypt', area: 'EG', currency: 'EGP' },
    KW: { country: 'Kuwait', area: 'KW', currency: 'KWD' },
    BH: { country: 'Bahrain', area: 'BH', currency: 'BHD' },
    LB: { country: 'Lebanon', area: 'LB', currency: 'LBP' },
    QA: { country: 'Qatar', area: 'QA', currency: 'QAR' },
    OM: { country: 'Oman', area: 'OM', currency: 'OMR' },
    JO: { country: 'Jordan', area: 'JO', currency: 'JOD' }
  },

  FX_TO_USD: {
    USD: 1,
    AED: 0.2723,
    SAR: 0.2667,
    EGP: 0.0204,
    KWD: 3.26,
    BHD: 2.65,
    LBP: 0.0000112,
    QAR: 0.2747,
    OMR: 2.5974,
    JOD: 1.4104
  },

  SOURCE_PRECEDENCE: {
    ELCINEMA_TITLE_BOXOFFICE: 100,
    ELCINEMA_CHART: 90,
    BOXOFFICEMOJO_INTL: 40,
    BOXOFFICEMOJO_AREA_YEAR: 35
  },

  FRESHNESS_DAYS: {
    ELCINEMA_TITLE_BOXOFFICE: 14,
    ELCINEMA_CHART: 7,
    BOXOFFICEMOJO_INTL: 7,
    BOXOFFICEMOJO_AREA_YEAR: 30
  },

  MATCHING: {
    AUTO_MATCH_THRESHOLD: 0.92,
    REVIEW_THRESHOLD: 0.75,
    YEAR_BONUS: 0.06,
    YEAR_PENALTY: 0.05,
    MAX_REVIEW_CANDIDATES: 5
  },

  PIPELINE: {
    FETCH_MOJO_AREA_YEARS: 2,
    EL_CINEMA_CHART_LIMIT: 25,
    HTTP_SLEEP_MS: 900,
    MAX_HTTP_RETRIES: 2
  },

  LOOKUP: {
    DEFAULT_QUERY_CELL: 'B3',
    TITLE_CELL: 'B2'
  }
};

const SCHEMAS = {
  RAW: [
    'raw_key', 'run_id', 'fetched_at', 'source_name', 'source_url', 'parser_version', 'parser_confidence',
    'source_entity_id', 'country', 'country_code', 'film_title_raw', 'film_title_ar_raw', 'release_year_hint',
    'record_scope', 'record_granularity', 'record_semantics', 'source_confidence', 'match_confidence',
    'freshness_status', 'evidence_type', 'period_label_raw', 'period_start_date', 'period_end_date', 'period_key',
    'rank', 'period_gross_local', 'cumulative_gross_local', 'currency', 'admissions_actual', 'work_id',
    'distributor', 'notes', 'raw_payload_json'
  ],

  RECON: [
    'recon_key', 'film_id', 'canonical_title', 'canonical_title_ar', 'release_year', 'country', 'country_code',
    'record_scope', 'record_granularity', 'record_semantics', 'evidence_type', 'period_label_raw',
    'period_start_date', 'period_end_date', 'period_key', 'rank', 'period_gross_local', 'period_gross_usd',
    'cumulative_gross_local', 'cumulative_gross_usd', 'currency', 'approx_ticket_estimate', 'ticket_estimate_confidence',
    'source_name', 'source_precedence', 'source_confidence', 'match_confidence', 'freshness_status', 'parser_confidence',
    'winning_raw_key', 'alternate_raw_keys_json', 'source_url', 'notes', 'reconciled_at'
  ],

  FILMS: [
    'film_id', 'canonical_title', 'canonical_title_ar', 'release_year', 'identity_confidence', 'metadata_source',
    'created_at', 'updated_at'
  ],

  ALIASES: [
    'alias_id', 'film_id', 'alias_text', 'normalized_alias', 'alias_language', 'alias_type',
    'confidence', 'source', 'needs_review', 'created_at'
  ],

  REVIEW: [
    'review_id', 'created_at', 'review_type', 'status', 'severity', 'film_title_raw', 'film_id_candidate',
    'country', 'period_key', 'source_name', 'source_url', 'details_json', 'analyst_notes'
  ],

  RUN_LOG: [
    'run_id', 'started_at', 'completed_at', 'pipeline_version', 'source_attempts', 'source_successes',
    'source_failures', 'rows_fetched', 'raw_added', 'raw_skipped', 'reconciled_written', 'review_items_added',
    'parser_warnings', 'anomalies', 'notes'
  ],

  TICKET_PRICES: [
    'price_id', 'country', 'country_code', 'currency', 'avg_ticket_price_local', 'avg_ticket_price_usd',
    'confidence', 'valid_from', 'valid_to', 'source', 'source_url', 'notes', 'updated_at'
  ],

  SOURCE_STATUS: [
    'checked_at', 'source_name', 'country', 'status', 'rows', 'message'
  ],

  CONFIG: ['key', 'value', 'notes']
};

const MONTH_MAP = {
  jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
  jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12
};

// ============================================================
// SECTION 2: MENUS / ENTRY POINTS
// ============================================================

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('🎬 MENA Intelligence')
    .addItem('Initialize Workbook', 'initializeWorkbook')
    .addItem('Run Pipeline', 'runPipeline')
    .addItem('Refresh Live Lookup', 'refreshLiveLookupFromSheet')
    .addItem('Search Title', 'searchTitleFromSheet')
    .addSeparator()
    .addItem('Open Dashboard', 'openDashboard')
    .addItem('Update Dashboard', 'updateDashboardFromSheet')
    .addSeparator()
    .addItem('Review Queue', 'openReviewQueue')
    .addItem('Test Sources', 'testSources')
    .addToUi();
}

function initializeWorkbook() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  seedSystemConfig_(ss);
  seedTicketPrices_(ss);
  setupLookupSheet_(ss);
  setupDashboardSheet_(ss);
  SpreadsheetApp.getUi().alert('Workbook initialized.');
}


function parseBomMarkets_() {
  var codes = parseCsvCodes_(getMasterConfigValue_('BOM_MARKETS', MASTER_PRODUCT.DEFAULTS.BOM_MARKETS)).filter(function(code) {
    return !!CONFIG.MARKETS[code];
  });
  return codes.length ? codes : ['AE'];
}

function parseBomYears_() {
  var years = parseConfiguredYears_(getMasterConfigValue_('BOM_YEARS', MASTER_PRODUCT.DEFAULTS.BOM_YEARS));
  if (!years.length) {
    var currentYear = new Date().getFullYear();
    years = [currentYear, currentYear - 1];
  }
  return years;
}

function runPipeline() {
  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    ensureAllSheets_(ss);

    const ctx = createRunContext_();
    const rawRecords = [];
    const statusRows = [];

    // 1) Box Office Mojo latest international chart
    runSource_(ctx, 'BOXOFFICEMOJO_INTL', '', () => {
      const rows = fetchBoxOfficeMojoIntlLatest_();
      rawRecords.push.apply(rawRecords, rows);
      statusRows.push([isoNow_(), 'BOXOFFICEMOJO_INTL', 'All', 'OK', rows.length, 'Latest international weekends fetched']);
      return rows.length;
    }, statusRows);

    // 2) Box Office Mojo area/year weekend toplines (current + previous year)
    const currentYear = new Date().getFullYear();
    const years = [];
    for (let i = 0; i < CONFIG.PIPELINE.FETCH_MOJO_AREA_YEARS; i++) years.push(currentYear - i);

    Object.keys(CONFIG.MARKETS).forEach(code => {
      years.forEach(year => {
        runSource_(ctx, 'BOXOFFICEMOJO_AREA_YEAR', code, () => {
          const rows = fetchBoxOfficeMojoAreaYear_(code, year);
          rawRecords.push.apply(rawRecords, rows);
          statusRows.push([isoNow_(), 'BOXOFFICEMOJO_AREA_YEAR', code, 'OK', rows.length, `Fetched ${year}`]);
          return rows.length;
        }, statusRows);
      });
    });

    // 3) elCinema current Egypt chart
    let elCinemaChartRows = [];
    runSource_(ctx, 'ELCINEMA_CHART', 'EG', () => {
      elCinemaChartRows = fetchElCinemaCurrentChart_();
      rawRecords.push.apply(rawRecords, elCinemaChartRows);
      statusRows.push([isoNow_(), 'ELCINEMA_CHART', 'EG', 'OK', elCinemaChartRows.length, 'Current Egypt chart fetched']);
      return elCinemaChartRows.length;
    }, statusRows);

    // 4) elCinema title pages for any chart titles with work ids
    const workIds = unique_(elCinemaChartRows.map(r => r.work_id).filter(Boolean));
    workIds.forEach(workId => {
      runSource_(ctx, 'ELCINEMA_TITLE_BOXOFFICE', 'MULTI', () => {
        const rows = fetchElCinemaTitleBoxOffice_(workId);
        rawRecords.push.apply(rawRecords, rows);
        statusRows.push([isoNow_(), 'ELCINEMA_TITLE_BOXOFFICE', 'MULTI', 'OK', rows.length, `work_id=${workId}`]);
        return rows.length;
      }, statusRows);
    });

    ctx.rowsFetched = rawRecords.length;

    // Persist raw
    const rawWrite = appendRawEvidence_(ss, rawRecords, ctx.runId);
    ctx.rawAdded = rawWrite.added;
    ctx.rawSkipped = rawWrite.skipped;

    // Title identity + reconciliation
    const allRaw = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RAW));
    const reconRows = reconcileEvidence_(ss, allRaw, ctx);
    rewriteSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RECON), SCHEMAS.RECON, reconRows);
    ctx.reconciledWritten = reconRows.length;

    appendRows_(ss.getSheetByName(CONFIG.SHEETS.SOURCE_STATUS), statusRows);
    finalizeRunLog_(ss, ctx);

    SpreadsheetApp.getUi().alert(
      `Pipeline complete.\nFetched: ${ctx.rowsFetched}\nRaw added: ${ctx.rawAdded}\nReconciled: ${ctx.reconciledWritten}\nReview items: ${ctx.reviewItemsAdded}`
    );
  } finally {
    lock.releaseLock();
  }
}

function searchTitleFromSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const existingSheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  const query = existingSheet ? String(existingSheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '').trim() : '';
  ensureAllSheets_(ss);
  if (!query) {
    SpreadsheetApp.getUi().alert(`Enter a title in ${CONFIG.LOOKUP.DEFAULT_QUERY_CELL}.`);
    return;
  }
  renderLookup_(query, false);
}

function refreshLiveLookupFromSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const existingSheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  const query = existingSheet ? String(existingSheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '').trim() : '';
  ensureAllSheets_(ss);
  if (!query) {
    SpreadsheetApp.getUi().alert(`Enter a title in ${CONFIG.LOOKUP.DEFAULT_QUERY_CELL}.`);
    return;
  }
  renderLookup_(query, true);
}

function openReviewQueue() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  ss.setActiveSheet(ss.getSheetByName(CONFIG.SHEETS.REVIEW));
}

function testSources() {
  const tests = [];
  try {
    tests.push(['BOXOFFICEMOJO_INTL', '', fetchBoxOfficeMojoIntlLatest_().length]);
  } catch (e) {
    tests.push(['BOXOFFICEMOJO_INTL', 'ERROR', e.message]);
  }
  try {
    tests.push(['BOXOFFICEMOJO_AREA_YEAR_AE', '', fetchBoxOfficeMojoAreaYear_('AE', new Date().getFullYear()).length]);
  } catch (e) {
    tests.push(['BOXOFFICEMOJO_AREA_YEAR_AE', 'ERROR', e.message]);
  }
  try {
    const chart = fetchElCinemaCurrentChart_();
    tests.push(['ELCINEMA_CHART', '', chart.length]);
    if (chart.length && chart[0].work_id) {
      tests.push(['ELCINEMA_TITLE_BOXOFFICE', '', fetchElCinemaTitleBoxOffice_(chart[0].work_id).length]);
    }
  } catch (e) {
    tests.push(['ELCINEMA', 'ERROR', e.message]);
  }
  const msg = tests.map(r => `${r[0]} → ${r[1] || r[2]}`).join('\n');
  SpreadsheetApp.getUi().alert(msg);
}

// ============================================================
// SECTION 3: SHEET SETUP
// ============================================================

function ensureAllSheets_(ss) {
  const ordered = [
    [CONFIG.SHEETS.LOOKUP, SCHEMAS.CONFIG],
    [CONFIG.SHEETS.DASHBOARD, SCHEMAS.CONFIG],
    [CONFIG.SHEETS.RAW, SCHEMAS.RAW],
    [CONFIG.SHEETS.RECON, SCHEMAS.RECON],
    [CONFIG.SHEETS.FILMS, SCHEMAS.FILMS],
    [CONFIG.SHEETS.ALIASES, SCHEMAS.ALIASES],
    [CONFIG.SHEETS.REVIEW, SCHEMAS.REVIEW],
    [CONFIG.SHEETS.RUN_LOG, SCHEMAS.RUN_LOG],
    [CONFIG.SHEETS.TICKET_PRICES, SCHEMAS.TICKET_PRICES],
    [CONFIG.SHEETS.SOURCE_STATUS, SCHEMAS.SOURCE_STATUS],
    [CONFIG.SHEETS.CONFIG, SCHEMAS.CONFIG]
  ];

  ordered.forEach(item => {
    const name = item[0];
    const schema = item[1];
    let sheet = ss.getSheetByName(name);
    if (!sheet) sheet = ss.insertSheet(name);
    if (name === CONFIG.SHEETS.LOOKUP || name === CONFIG.SHEETS.DASHBOARD) return;
    ensureHeader_(sheet, schema);
  });

  setupLookupSheet_(ss);
  setupDashboardSheet_(ss);
  cleanupDefaultSheets_(ss);
}

function ensureHeader_(sheet, schema) {
  const width = schema.length;
  const existing = sheet.getLastRow() >= 1 ? sheet.getRange(1, 1, 1, Math.max(width, 1)).getValues()[0] : [];
  const same = schema.every((v, i) => existing[i] === v);
  if (!same) {
    sheet.clear();
    sheet.getRange(1, 1, 1, width).setValues([schema]);
    sheet.getRange(1, 1, 1, width).setFontWeight('bold').setBackground('#111827').setFontColor('#ffffff');
    sheet.setFrozenRows(1);
    autoResize_(sheet, width);
  }
}

function setupLookupSheet_(ss, preserveQuery) {
  let sheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  if (!sheet) sheet = ss.insertSheet(CONFIG.SHEETS.LOOKUP);

  const existingQuery = preserveQuery !== undefined
    ? preserveQuery
    : String(sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '');

  if (!sheet.getRange('A1').getValue()) {
    sheet.clear();
  }

  sheet.getRange('A1').setValue('MENA Box Office Acquisition Lookup').setFontSize(15).setFontWeight('bold');
  sheet.getRange('A2').setValue('Title Query').setFontWeight('bold');
  sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL)
    .setBackground('#FFF9C4')
    .setFontWeight('bold')
    .setNote('Enter a film title, then use menu: Search Title or Refresh Live Lookup');
  if (existingQuery) sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).setValue(existingQuery);
  sheet.getRange('A4').setValue('Use this page for acquisition research.').setFontColor('#6B7280');
  sheet.setFrozenRows(4);
  sheet.setColumnWidths(1, 12, 150);
  sheet.setColumnWidth(2, 280);
}

function clearLookupOutput_(sheet) {
  const startRow = 6;
  const maxRows = Math.max(sheet.getMaxRows() - startRow + 1, 1);
  const maxCols = Math.max(sheet.getMaxColumns(), 16);
  sheet.getRange(startRow, 1, maxRows, maxCols).clearContent().clearFormat();
}


function setupDashboardSheet_(ss, preserveQuery, preserveCountry) {
  let sheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  if (!sheet) sheet = ss.insertSheet(CONFIG.SHEETS.DASHBOARD);

  const existingQuery = preserveQuery !== undefined ? preserveQuery : String(sheet.getRange('B2').getValue() || '');
  const existingCountry = preserveCountry !== undefined ? preserveCountry : String(sheet.getRange('B3').getValue() || 'ALL');

  if (!sheet.getRange('A1').getValue()) {
    sheet.clear();
  }

  sheet.getRange('A1').setValue('MENA Box Office Performance Dashboard').setFontSize(15).setFontWeight('bold');
  sheet.getRange('A2').setValue('Title Query').setFontWeight('bold');
  sheet.getRange('A3').setValue('Country Filter').setFontWeight('bold');
  sheet.getRange('A4').setValue('Use this dashboard to inspect weekly performance and trend lines for a selected title.').setFontColor('#6B7280');

  sheet.getRange('B2').setBackground('#DCFCE7').setFontWeight('bold').setNote('Enter a film title and use Update Dashboard from the custom menu.');
  if (existingQuery) sheet.getRange('B2').setValue(existingQuery);

  const countryOptions = ['ALL'].concat(Object.keys(CONFIG.MARKETS));
  const dv = SpreadsheetApp.newDataValidation().requireValueInList(countryOptions, true).setAllowInvalid(false).build();
  sheet.getRange('B3').setDataValidation(dv).setBackground('#DBEAFE');
  sheet.getRange('B3').setValue(countryOptions.indexOf(existingCountry) >= 0 ? existingCountry : 'ALL');

  sheet.setFrozenRows(4);
  sheet.setColumnWidths(1, 14, 150);
  sheet.setColumnWidth(2, 240);
}

function clearDashboardOutput_(sheet) {
  const maxRows = Math.max(sheet.getMaxRows() - 5, 1);
  const maxCols = Math.max(sheet.getMaxColumns(), 16);
  sheet.getRange(6, 1, maxRows, maxCols).clearContent().clearFormat();
  const charts = sheet.getCharts();
  charts.forEach(chart => sheet.removeChart(chart));
}

function openDashboard() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  ss.setActiveSheet(ss.getSheetByName(CONFIG.SHEETS.DASHBOARD));
}

function updateDashboardFromSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  const sheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  const query = String(sheet.getRange('B2').getValue() || '').trim();
  const countryCode = String(sheet.getRange('B3').getValue() || 'ALL').trim() || 'ALL';
  if (!query) {
    SpreadsheetApp.getUi().alert('Enter a title in B2 on the Performance Dashboard sheet.');
    return;
  }
  renderDashboard_(query, countryCode);
}

function renderDashboard_(query, countryCode) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  const sheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  setupDashboardSheet_(ss, query, countryCode);
  clearDashboardOutput_(sheet);

  const recon = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RECON));
  const films = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.FILMS));
  const aliases = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.ALIASES));
  const match = matchQueryToFilm_(query, films, aliases);

  let relevant = [];
  const normQueryEn = normalizeLatinTitle_(query);
  const normQueryAr = normalizeArabicTitle_(query);

  if (match.film_id) {
    relevant = recon.filter(r => r.film_id === match.film_id);
  } else {
    relevant = recon.filter(r => {
      const t1 = normalizeLatinTitle_(r.canonical_title || r.film_title_raw || '');
      const t2 = normalizeArabicTitle_(r.canonical_title_ar || r.film_title_ar_raw || '');
      return (t1 && (t1.indexOf(normQueryEn) >= 0 || normQueryEn.indexOf(t1) >= 0 || titleSimilarity_(normQueryEn, t1) >= 0.75)) ||
             (t2 && (t2.indexOf(normQueryAr) >= 0 || normQueryAr.indexOf(t2) >= 0 || titleSimilarity_(normQueryAr, t2) >= 0.80));
    });
  }

  const comparable = relevant
    .filter(r => r.record_scope === 'title' && r.record_semantics === 'title_period_gross')
    .slice();

  comparable.sort(compareDashboardPeriods_);

  const coverageCounts = {};
  comparable.forEach(r => coverageCounts[r.country_code] = (coverageCounts[r.country_code] || 0) + 1);

  let chosenCountry = countryCode && countryCode !== 'ALL' ? countryCode : '';
  if (!chosenCountry) {
    let bestCode = '';
    let bestCount = -1;
    Object.keys(coverageCounts).forEach(code => {
      if (coverageCounts[code] > bestCount) {
        bestCode = code;
        bestCount = coverageCounts[code];
      }
    });
    chosenCountry = bestCode || 'ALL';
  }

  sheet.getRange('B2').setValue(query);
  sheet.getRange('B3').setValue(chosenCountry || 'ALL');

  const filtered = chosenCountry === 'ALL' ? comparable : comparable.filter(r => r.country_code === chosenCountry);
  const chartRows = filtered
    .slice()
    .sort(compareDashboardPeriods_)
    .map((r, idx, arr) => {
      const displayPeriod = dashboardPeriodLabel_(r, idx, arr);
      return {
        display_period: displayPeriod,
        period_start_date: normalizeDashboardDateValue_(r.period_start_date),
        period_end_date: normalizeDashboardDateValue_(r.period_end_date),
        period_key: r.period_key || r.period_label_raw || '',
        country: r.country || '',
        country_code: r.country_code || '',
        record_granularity: r.record_granularity || '',
        gross: Number(r.period_gross_local || 0),
        currency: r.currency || '',
        source_name: r.source_name || ''
      };
    });

  const totalGross = chartRows.reduce((sum, r) => sum + (Number(r.gross) || 0), 0);
  const peakRow = chartRows.slice().sort((a, b) => (Number(b.gross || 0) - Number(a.gross || 0)))[0] || null;
  const latestRow = chartRows[chartRows.length - 1] || null;

  const summary = [
    ['Matched Film', match.title || query, 'Match Confidence', match.score || ''],
    ['Country Shown', chosenCountry === 'ALL' ? 'All comparable markets' : ((CONFIG.MARKETS[chosenCountry] && CONFIG.MARKETS[chosenCountry].country) || chosenCountry), 'Records', chartRows.length],
    ['Total Period Gross', totalGross || '', 'Currency', chartRows.length ? unique_(chartRows.map(r => r.currency).filter(Boolean)).join(', ') : ''],
    ['Peak Period', peakRow ? peakRow.display_period : '', 'Peak Gross', peakRow ? peakRow.gross : ''],
    ['Latest Period', latestRow ? latestRow.display_period : '', 'Latest Gross', latestRow ? latestRow.gross : '']
  ];
  sheet.getRange(6, 1, summary.length, 4).setValues(summary);
  sheet.getRange(6, 1, 1, 4).setFontWeight('bold').setBackground('#E5E7EB');

  const noteRow = 12;
  let note = '';
  if (!comparable.length) {
    note = 'No title-level performance rows are available for this title yet.';
  } else if (chosenCountry === 'ALL') {
    note = 'Country filter is ALL. The table may include multiple markets and currencies, so the chart is hidden until you choose one market.';
  } else {
    note = 'Rows are sorted by actual period dates. Weekend markets are shown with normalized weekend labels.';
  }
  sheet.getRange(noteRow, 1).setValue('Dashboard Notes').setFontWeight('bold').setBackground('#FDE68A');
  sheet.getRange(noteRow + 1, 1).setValue(note);

  const marketSummaryRow = noteRow + 3;
  sheet.getRange(marketSummaryRow, 1).setValue('Available Market Coverage').setFontWeight('bold').setBackground('#DBEAFE');
  const marketHeader = [['Country', 'Code', 'Records', 'First Period', 'Last Period']];
  sheet.getRange(marketSummaryRow + 1, 1, 1, marketHeader[0].length).setValues(marketHeader).setFontWeight('bold');
  const marketTable = Object.keys(coverageCounts).sort().map(code => {
    const rows = comparable.filter(r => r.country_code === code).sort(compareDashboardPeriods_);
    const firstLabel = rows.length ? dashboardPeriodLabel_(rows[0], 0, rows) : '';
    const lastLabel = rows.length ? dashboardPeriodLabel_(rows[rows.length - 1], rows.length - 1, rows) : '';
    return [
      (CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code,
      code,
      rows.length,
      firstLabel,
      lastLabel
    ];
  });
  if (marketTable.length) sheet.getRange(marketSummaryRow + 2, 1, marketTable.length, marketHeader[0].length).setValues(marketTable);
  else sheet.getRange(marketSummaryRow + 2, 1).setValue('No market coverage found.');

  const weeklyRow = marketSummaryRow + 4 + Math.max(marketTable.length, 1);
  sheet.getRange(weeklyRow, 1).setValue('Performance Table').setFontWeight('bold').setBackground('#D1FAE5');
  const weeklyHeader = [['Period', 'Start Date', 'End Date', 'Country', 'Period Gross (Local)', 'Currency', 'Source']];
  sheet.getRange(weeklyRow + 1, 1, 1, weeklyHeader[0].length).setValues(weeklyHeader).setFontWeight('bold');
  const weeklyValues = chartRows.map(r => [r.display_period, r.period_start_date, r.period_end_date, r.country, r.gross, r.currency, r.source_name]);
  if (weeklyValues.length) {
    sheet.getRange(weeklyRow + 2, 1, weeklyValues.length, weeklyHeader[0].length).setValues(weeklyValues);
  } else {
    sheet.getRange(weeklyRow + 2, 1).setValue('No rows for the selected title / market.');
  }

  try {
    sheet.showColumns(1, 20);
  } catch (e) {}

  if (weeklyValues.length && chosenCountry !== 'ALL') {
    const chartDataHeader = [['Period', 'Period Gross']];
    const chartDataValues = chartRows.map(r => [r.display_period, r.gross]);
    sheet.getRange(weeklyRow + 1, 13, 1, 2).setValues(chartDataHeader).setFontWeight('bold');
    sheet.getRange(weeklyRow + 2, 13, chartDataValues.length, 2).setValues(chartDataValues);

    const chartBuilder = sheet.newChart()
      .asLineChart()
      .addRange(sheet.getRange(weeklyRow + 1, 13, chartDataValues.length + 1, 2))
      .setPosition(6, 6, 0, 0)
      .setOption('title', 'Performance Trend')
      .setOption('legend', { position: 'none' })
      .setOption('hAxis', { title: 'Period' })
      .setOption('vAxis', { title: 'Gross (' + (chartRows[0].currency || 'Local') + ')' });
    sheet.insertChart(chartBuilder.build());
    sheet.hideColumns(13, 2);
  }

  if (weeklyValues.length) {
    sheet.getRange(weeklyRow + 2, 2, weeklyValues.length, 2).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(weeklyRow + 2, 5, weeklyValues.length, 1).setNumberFormat('#,##0.00');
  }
  autoResize_(sheet, 12);
}

function compareDashboardPeriods_(a, b) {
  const ac = String(a.country_code || '');
  const bc = String(b.country_code || '');
  if (ac !== bc) return ac.localeCompare(bc);

  const aStart = dashboardSortValue_(a.period_start_date, a.period_key);
  const bStart = dashboardSortValue_(b.period_start_date, b.period_key);
  if (aStart !== bStart) return aStart - bStart;

  const aEnd = dashboardSortValue_(a.period_end_date, a.period_key);
  const bEnd = dashboardSortValue_(b.period_end_date, b.period_key);
  if (aEnd !== bEnd) return aEnd - bEnd;

  return String(a.period_key || a.period_label_raw || '').localeCompare(String(b.period_key || b.period_label_raw || ''));
}

function dashboardSortValue_(value, fallbackKey) {
  if (Object.prototype.toString.call(value) === '[object Date]') return value.getTime();
  const s = String(value || '').trim();
  if (s) {
    const d = new Date(s);
    if (!isNaN(d.getTime())) return d.getTime();
    const weekMatch = s.match(/^(\d{4})-W(\d{1,2})$/i);
    if (weekMatch) return isoWeekStartDate_(parseInt(weekMatch[1], 10), parseInt(weekMatch[2], 10)).getTime();
  }
  const f = String(fallbackKey || '').trim();
  const weekMatch = f.match(/^(\d{4})-W(\d{1,2})$/i);
  if (weekMatch) return isoWeekStartDate_(parseInt(weekMatch[1], 10), parseInt(weekMatch[2], 10)).getTime();
  return 0;
}

function normalizeDashboardDateValue_(value) {
  if (!value) return '';
  if (Object.prototype.toString.call(value) === '[object Date]') return value;
  const d = new Date(String(value));
  return isNaN(d.getTime()) ? String(value) : d;
}

function dashboardPeriodLabel_(row, idx, rows) {
  const granularity = String(row.record_granularity || '').toLowerCase();
  const key = String(row.period_key || row.period_label_raw || '').trim();

  if (granularity === 'week') {
    const match = key.match(/^(\d{4})-W(\d{1,2})$/i);
    if (match) return 'Week ' + parseInt(match[2], 10);
    return key || ('Week ' + (idx + 1));
  }

  if (granularity === 'weekend') {
    const dt = row.period_end_date || row.period_start_date || '';
    const weekendInfo = isoWeekendLabelFromValue_(dt);
    if (weekendInfo) return weekendInfo;
    return 'Weekend ' + (idx + 1);
  }

  return key || ('Period ' + (idx + 1));
}

function isoWeekendLabelFromValue_(value) {
  let d = null;
  if (Object.prototype.toString.call(value) === '[object Date]') d = value;
  else {
    const s = String(value || '').trim();
    if (!s) return '';
    const parsed = new Date(s);
    if (!isNaN(parsed.getTime())) d = parsed;
  }
  if (!d) return '';
  const info = isoWeekInfo_(d);
  return 'Weekend ' + info.week;
}

function isoWeekInfo_(dateObj) {
  const d = new Date(Date.UTC(dateObj.getFullYear(), dateObj.getMonth(), dateObj.getDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const year = d.getUTCFullYear();
  const yearStart = new Date(Date.UTC(year, 0, 1));
  const week = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return { year: year, week: week };
}

function isoWeekStartDate_(year, week) {
  const simple = new Date(Date.UTC(year, 0, 1 + (week - 1) * 7));
  const dow = simple.getUTCDay() || 7;
  if (dow <= 4) simple.setUTCDate(simple.getUTCDate() - dow + 1);
  else simple.setUTCDate(simple.getUTCDate() + 8 - dow);
  return simple;
}


function seedSystemConfig_(ss) {
  const sheet = ss.getSheetByName(CONFIG.SHEETS.CONFIG);
  const rows = [
    ['version', CONFIG.VERSION, 'Workbook version'],
    ['freshness_rule', 'Freshness is source-specific and not equivalent across source types', 'Informational'],
    ['ticket_estimate_rule', 'Only shown when ticket-price confidence is high and evidence is title-performance', 'Informational']
  ];
  rewriteSheetObjects_(sheet, SCHEMAS.CONFIG, rows.map(r => ({ key: r[0], value: r[1], notes: r[2] })));
}

function seedTicketPrices_(ss) {
  const sheet = ss.getSheetByName(CONFIG.SHEETS.TICKET_PRICES);
  const existing = readSheetObjects_(sheet);
  if (existing.length) return;

  const seed = [
    { price_id: 'tp_AE_1', country: 'United Arab Emirates', country_code: 'AE', currency: 'AED', avg_ticket_price_local: 40, avg_ticket_price_usd: 40 * CONFIG.FX_TO_USD.AED, confidence: 'medium', valid_from: '2025-01-01', valid_to: '2026-12-31', source: 'internal_estimate', source_url: '', notes: 'Average blended cinema ticket assumption', updated_at: isoNow_() },
    { price_id: 'tp_SA_1', country: 'Saudi Arabia', country_code: 'SA', currency: 'SAR', avg_ticket_price_local: 50, avg_ticket_price_usd: 50 * CONFIG.FX_TO_USD.SAR, confidence: 'medium', valid_from: '2025-01-01', valid_to: '2026-12-31', source: 'internal_estimate', source_url: '', notes: 'Average blended cinema ticket assumption', updated_at: isoNow_() },
    { price_id: 'tp_EG_1', country: 'Egypt', country_code: 'EG', currency: 'EGP', avg_ticket_price_local: 110, avg_ticket_price_usd: 110 * CONFIG.FX_TO_USD.EGP, confidence: 'medium', valid_from: '2025-01-01', valid_to: '2026-12-31', source: 'internal_estimate', source_url: '', notes: 'Average blended cinema ticket assumption', updated_at: isoNow_() },
    { price_id: 'tp_KW_1', country: 'Kuwait', country_code: 'KW', currency: 'KWD', avg_ticket_price_local: 3.5, avg_ticket_price_usd: 3.5 * CONFIG.FX_TO_USD.KWD, confidence: 'low', valid_from: '2025-01-01', valid_to: '2026-12-31', source: 'internal_estimate', source_url: '', notes: 'Average blended cinema ticket assumption', updated_at: isoNow_() },
    { price_id: 'tp_BH_1', country: 'Bahrain', country_code: 'BH', currency: 'BHD', avg_ticket_price_local: 4, avg_ticket_price_usd: 4 * CONFIG.FX_TO_USD.BHD, confidence: 'low', valid_from: '2025-01-01', valid_to: '2026-12-31', source: 'internal_estimate', source_url: '', notes: 'Average blended cinema ticket assumption', updated_at: isoNow_() },
    { price_id: 'tp_LB_1', country: 'Lebanon', country_code: 'LB', currency: 'LBP', avg_ticket_price_local: 650000, avg_ticket_price_usd: 650000 * CONFIG.FX_TO_USD.LBP, confidence: 'low', valid_from: '2025-01-01', valid_to: '2026-12-31', source: 'internal_estimate', source_url: '', notes: 'Average blended cinema ticket assumption', updated_at: isoNow_() },
    { price_id: 'tp_QA_1', country: 'Qatar', country_code: 'QA', currency: 'QAR', avg_ticket_price_local: 35, avg_ticket_price_usd: 35 * CONFIG.FX_TO_USD.QAR, confidence: 'low', valid_from: '2025-01-01', valid_to: '2026-12-31', source: 'internal_estimate', source_url: '', notes: 'Average blended cinema ticket assumption', updated_at: isoNow_() },
    { price_id: 'tp_OM_1', country: 'Oman', country_code: 'OM', currency: 'OMR', avg_ticket_price_local: 3.5, avg_ticket_price_usd: 3.5 * CONFIG.FX_TO_USD.OMR, confidence: 'low', valid_from: '2025-01-01', valid_to: '2026-12-31', source: 'internal_estimate', source_url: '', notes: 'Average blended cinema ticket assumption', updated_at: isoNow_() },
    { price_id: 'tp_JO_1', country: 'Jordan', country_code: 'JO', currency: 'JOD', avg_ticket_price_local: 6, avg_ticket_price_usd: 6 * CONFIG.FX_TO_USD.JOD, confidence: 'low', valid_from: '2025-01-01', valid_to: '2026-12-31', source: 'internal_estimate', source_url: '', notes: 'Average blended cinema ticket assumption', updated_at: isoNow_() }
  ];
  appendObjects_(sheet, SCHEMAS.TICKET_PRICES, seed);
}

function cleanupDefaultSheets_(ss) {
  ss.getSheets().forEach(sh => {
    if ((sh.getName() === 'Sheet1' || sh.getName() === 'Sheet 1') && ss.getSheets().length > 1) {
      ss.deleteSheet(sh);
    }
  });
}

// ============================================================
// SECTION 4: HTTP / SOURCE EXECUTION
// ============================================================

function runSource_(ctx, sourceName, countryCode, fn, statusRows) {
  ctx.sourceAttempts++;
  try {
    const count = fn();
    ctx.sourceSuccesses++;
    return count;
  } catch (e) {
    ctx.sourceFailures++;
    statusRows.push([isoNow_(), sourceName, countryCode || '', 'ERROR', 0, e.message]);
    ctx.notes.push(`${sourceName}${countryCode ? ' [' + countryCode + ']' : ''}: ${e.message}`);
    return 0;
  }
}

function fetchUrl_(url) {
  let lastErr = null;
  const maxRetries = Math.max(Number(CONFIG.PIPELINE.MAX_HTTP_RETRIES || 0), 0);
  for (let i = 0; i <= maxRetries; i++) {
    try {
      const resp = UrlFetchApp.fetch(url, {
        muteHttpExceptions: true,
        followRedirects: true,
        headers: {
          'User-Agent': CONFIG.USER_AGENT,
          'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
          'Cache-Control': 'no-cache'
        }
      });
      const code = resp.getResponseCode();
      if (code >= 200 && code < 300) {
        const body = resp.getContentText() || '';
        if (!body.trim()) throw new Error('Empty response body');
        Utilities.sleep(CONFIG.PIPELINE.HTTP_SLEEP_MS);
        return body;
      }

      const retryable = code === 408 || code === 425 || code === 429 || (code >= 500 && code < 600);
      lastErr = new Error(`HTTP ${code} for ${url}`);
      if (!retryable || i === maxRetries) break;
    } catch (e) {
      lastErr = e;
      if (i === maxRetries) break;
    }
    const backoff = Math.min(12000, (Math.pow(2, i) * 900) + Math.floor(Math.random() * 500));
    Utilities.sleep(backoff);
  }
  throw lastErr || new Error(`Failed to fetch ${url}`);
}

// ============================================================
// SECTION 5: SOURCE PARSERS
// ============================================================

function fetchBoxOfficeMojoIntlLatest_() {
  const url = 'https://www.boxofficemojo.com/intl/';
  const html = fetchUrl_(url);
  const rows = [];
  const cleaned = normalizeWhitespace_(html);

  // Defensive row parsing from HTML table rows.
  const trMatches = cleaned.match(/<tr[\s\S]*?<\/tr>/gi) || [];
  let currentWeekendEnd = '';
  trMatches.forEach(tr => {
    const plain = htmlToText_(tr).trim();
    if (!plain) return;

    const weekendHdr = plain.match(/^Weekend Ending\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})$/i);
    if (weekendHdr) {
      currentWeekendEnd = toIsoDateFromLong_(weekendHdr[1]);
      return;
    }

    const tds = tr.match(/<td[\s\S]*?<\/td>/gi) || [];
    if (tds.length < 5) return;

    const cells = tds.map(c => htmlToText_(c).trim()).filter(Boolean);
    if (cells.length < 5) return;

    const country = cells[0];
    const marketCode = findMarketCodeByCountry_(country);
    if (!marketCode) return;

    const weekendLabel = cells[1];
    const releases = parseIntSafe_(cells[2]);
    const topTitle = cells[3];
    const distributor = cells[4] || '';
    const gross = parseMoney_(cells[cells.length - 1]);
    const period = parseMojoWeekendLabel_(weekendLabel, currentWeekendEnd ? parseInt(currentWeekendEnd.slice(0, 4), 10) : new Date().getFullYear());

    const payload = {
      country,
      weekendLabel,
      releases,
      topTitle,
      distributor,
      gross,
      currentWeekendEnd
    };

    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_INTL',
      source_url: url,
      parser_confidence: 0.78,
      source_entity_id: `${marketCode}_${period ? period.start : weekendLabel}`,
      country,
      country_code: marketCode,
      film_title_raw: topTitle,
      film_title_ar_raw: '',
      release_year_hint: currentWeekendEnd ? parseInt(currentWeekendEnd.slice(0, 4), 10) : '',
      record_scope: 'market',
      record_granularity: 'weekend',
      record_semantics: 'market_chart_topline',
      source_confidence: 0.45,
      match_confidence: 0.25,
      evidence_type: 'chart_mention',
      period_label_raw: weekendLabel,
      period_start_date: period ? period.start : '',
      period_end_date: period ? period.end : '',
      period_key: period ? period.key : '',
      rank: 1,
      period_gross_local: gross,
      cumulative_gross_local: '',
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor,
      notes: `Latest intl chart top title; releases=${releases}`,
      raw_payload_json: JSON.stringify(payload)
    }));
  });

  return dedupRawRecords_(rows);
}

function fetchBoxOfficeMojoAreaYear_(countryCode, year) {
  const market = CONFIG.MARKETS[countryCode];
  if (!market) throw new Error(`Unknown market ${countryCode}`);
  const url = `https://www.boxofficemojo.com/weekend/by-year/?area=${countryCode}`;
  const html = fetchUrl_(url);
  const rows = [];
  const trMatches = normalizeWhitespace_(html).match(/<tr[\s\S]*?<\/tr>/gi) || [];

  trMatches.forEach(tr => {
    const tds = tr.match(/<td[\s\S]*?<\/td>/gi) || [];
    if (tds.length < 7) return;
    const cells = tds.map(c => htmlToText_(c).trim());
    const label = cells[0];
    const period = parseMojoWeekendLabel_(label, year);
    if (!period || String(period.start).slice(0, 4) !== String(year)) return;

    const top10Gross = parseMoney_(cells[1]);
    const overallGross = parseMoney_(cells[3]);
    const releases = parseIntSafe_(cells[4]);
    const topTitle = cells[6] || '';

    const payload = { label, top10Gross, overallGross, releases, topTitle, year };
    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_AREA_YEAR',
      source_url: url,
      parser_confidence: 0.86,
      source_entity_id: `${countryCode}_${period.start}`,
      country: market.country,
      country_code: countryCode,
      film_title_raw: topTitle,
      film_title_ar_raw: '',
      release_year_hint: year,
      record_scope: 'market',
      record_granularity: 'weekend',
      record_semantics: 'market_chart_topline',
      source_confidence: 0.42,
      match_confidence: 0.25,
      evidence_type: 'chart_mention',
      period_label_raw: label,
      period_start_date: period.start,
      period_end_date: period.end,
      period_key: period.key,
      rank: 1,
      period_gross_local: top10Gross || overallGross,
      cumulative_gross_local: '',
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor: '',
      notes: `Area-year weekend chart top title; releases=${releases}`,
      raw_payload_json: JSON.stringify(payload)
    }));
  });

  return dedupRawRecords_(rows);
}

function fetchElCinemaCurrentChart_() {
  const url = 'https://elcinema.com/en/boxoffice/';
  const html = fetchUrl_(url);
  const rows = [];
  const chartWeek = detectElCinemaWeekLabel_(html);
  const period = chartWeek ? parseIsoWeek_(chartWeek) : null;

  // Parse visible cards/sections using work links + nearby revenue labels.
  const workRx = /href="\/en\/work\/(\d+)\/"[^>]*>([\s\S]*?)<\/a>/gi;
  const workEntries = [];
  let m;
  while ((m = workRx.exec(html)) !== null) {
    const id = m[1];
    const title = htmlToText_(m[2]).trim();
    if (!title || title.length < 2) continue;
    if (/^(Playing in which Cinemas\?|Revenue Details|Buy tickets)$/i.test(title)) continue;
    if (/^[\d\.]+$/.test(title)) continue;
    workEntries.push({ id, title, idx: m.index });
  }

  const uniqueEntries = [];
  const seen = {};
  workEntries.forEach(entry => {
    if (!seen[entry.id]) {
      seen[entry.id] = true;
      uniqueEntries.push(entry);
    }
  });

  uniqueEntries.slice(0, CONFIG.PIPELINE.EL_CINEMA_CHART_LIMIT).forEach((entry, idx) => {
    const nextIdx = idx + 1 < uniqueEntries.length ? uniqueEntries[idx + 1].idx : Math.min(entry.idx + 7000, html.length);
    const chunk = html.slice(entry.idx, nextIdx);
    const weeklyMatch = htmlToText_(chunk).match(/Weekly Revenue:\s*([\d,]+)\s*(EGP|SAR|USD|AED|KWD|BHD|LBP|QAR|OMR|JOD)/i);
    const totalMatch = htmlToText_(chunk).match(/Total Revenue:\s*([\d,]+)\s*(EGP|SAR|USD|AED|KWD|BHD|LBP|QAR|OMR|JOD)/i);
    const ratingMatch = htmlToText_(chunk).match(/^(\d+(?:\.\d+)?)\s+/);

    if (!weeklyMatch) return;
    const currency = weeklyMatch[2].toUpperCase();
    const weekly = parseMoney_(weeklyMatch[1]);
    const total = totalMatch ? parseMoney_(totalMatch[1]) : '';
    const payload = {
      work_id: entry.id,
      title: entry.title,
      chartWeek,
      weekly,
      total,
      rating: ratingMatch ? ratingMatch[1] : ''
    };

    rows.push(buildRawRecord_({
      source_name: 'ELCINEMA_CHART',
      source_url: url,
      parser_confidence: totalMatch ? 0.92 : 0.80,
      source_entity_id: entry.id,
      country: 'Egypt',
      country_code: 'EG',
      film_title_raw: entry.title,
      film_title_ar_raw: '',
      release_year_hint: period ? parseInt(period.start.slice(0, 4), 10) : '',
      record_scope: 'title',
      record_granularity: 'week',
      record_semantics: 'title_period_gross',
      source_confidence: 0.82,
      match_confidence: 0.50,
      evidence_type: 'title_performance',
      period_label_raw: chartWeek || '',
      period_start_date: period ? period.start : '',
      period_end_date: period ? period.end : '',
      period_key: period ? period.key : '',
      rank: idx + 1,
      period_gross_local: weekly,
      cumulative_gross_local: total,
      currency,
      admissions_actual: '',
      work_id: entry.id,
      distributor: '',
      notes: 'Current elCinema chart record',
      raw_payload_json: JSON.stringify(payload)
    }));
  });

  return dedupRawRecords_(rows);
}

function fetchElCinemaTitleBoxOffice_(workId) {
  const url = `https://elcinema.com/en/work/${workId}/boxoffice`;
  const html = fetchUrl_(url);
  const rows = [];
  const pageText = htmlToText_(html);
  const titleMatch = pageText.match(/Box Office:\s*Movie\s*-\s*(.*?)\s*-\s*(20\d{2})/i);
  const title = titleMatch ? titleMatch[1].trim() : '';
  const releaseYear = titleMatch ? parseInt(titleMatch[2], 10) : '';

  const sectionRx = /<h3[^>]*>\s*([^<]+?)\s*<\/h3>([\s\S]*?)(?=<h3[^>]*>|$)/gi;
  let sm;
  while ((sm = sectionRx.exec(html)) !== null) {
    const countryLabel = htmlToText_(sm[1]).trim();
    const marketCode = findMarketCodeByCountry_(countryLabel);
    if (!marketCode) continue;
    const sectionHtml = sm[2];
    const sectionText = htmlToText_(sectionHtml);

    const weekRows = [];
    const lineRx = /(\d{1,2}|5[0-3])\s+(20\d{2})\s+([\d,]+(?:\.\d+)?)\s+(EGP|SAR|USD|AED|KWD|BHD|LBP|QAR|OMR|JOD)/gi;
    let lm;
    while ((lm = lineRx.exec(sectionText)) !== null) {
      weekRows.push({ week: lm[1], year: lm[2], amount: parseMoney_(lm[3]), currency: lm[4] });
    }
    const totalMatch = sectionText.match(/Total\s+([\d,]+(?:\.\d+)?)\s+(EGP|SAR|USD|AED|KWD|BHD|LBP|QAR|OMR|JOD)/i);
    const totalAmount = totalMatch ? parseMoney_(totalMatch[1]) : '';

    if (!weekRows.length && !totalAmount) continue;

    weekRows.forEach((wr, idx) => {
      const period = parseIsoWeek_(`${wr.year}-W${String(wr.week).padStart(2, '0')}`);
      const payload = { workId, title, countryLabel, week: wr.week, year: wr.year, amount: wr.amount, totalAmount };
      rows.push(buildRawRecord_({
        source_name: 'ELCINEMA_TITLE_BOXOFFICE',
        source_url: url,
        parser_confidence: 0.94,
        source_entity_id: `${workId}_${marketCode}_${wr.year}_${wr.week}`,
        country: CONFIG.MARKETS[marketCode].country,
        country_code: marketCode,
        film_title_raw: title,
        film_title_ar_raw: '',
        release_year_hint: releaseYear,
        record_scope: 'title',
        record_granularity: 'week',
        record_semantics: 'title_period_gross',
        source_confidence: 0.90,
        match_confidence: 0.70,
        evidence_type: 'title_performance',
        period_label_raw: `${wr.year}-W${String(wr.week).padStart(2, '0')}`,
        period_start_date: period ? period.start : '',
        period_end_date: period ? period.end : '',
        period_key: period ? period.key : '',
        rank: idx + 1,
        period_gross_local: wr.amount,
        cumulative_gross_local: totalAmount,
        currency: wr.currency,
        admissions_actual: '',
        work_id: workId,
        distributor: '',
        notes: 'elCinema title box office weekly record',
        raw_payload_json: JSON.stringify(payload)
      }));
    });

    if (!weekRows.length && totalAmount) {
      rows.push(buildRawRecord_({
        source_name: 'ELCINEMA_TITLE_BOXOFFICE',
        source_url: url,
        parser_confidence: 0.70,
        source_entity_id: `${workId}_${marketCode}_TOTAL`,
        country: CONFIG.MARKETS[marketCode].country,
        country_code: marketCode,
        film_title_raw: title,
        film_title_ar_raw: '',
        release_year_hint: releaseYear,
        record_scope: 'title',
        record_granularity: 'lifetime',
        record_semantics: 'title_cumulative_total',
        source_confidence: 0.72,
        match_confidence: 0.70,
        evidence_type: 'title_performance',
        period_label_raw: 'lifetime',
        period_start_date: '',
        period_end_date: '',
        period_key: '',
        rank: '',
        period_gross_local: '',
        cumulative_gross_local: totalAmount,
        currency: totalMatch ? totalMatch[2].toUpperCase() : '',
        admissions_actual: '',
        work_id: workId,
        distributor: '',
        notes: 'elCinema title total only record',
        raw_payload_json: JSON.stringify({ workId, title, countryLabel, totalAmount })
      }));
    }
  }

  if (!rows.length) {
    enqueueReview_(SpreadsheetApp.getActiveSpreadsheet(), {
      review_type: 'parser_anomaly',
      severity: 'medium',
      film_title_raw: title || `work:${workId}`,
      film_id_candidate: '',
      country: '',
      period_key: '',
      source_name: 'ELCINEMA_TITLE_BOXOFFICE',
      source_url: url,
      details_json: JSON.stringify({ message: 'No parsable sections found on elCinema title page', workId: workId }),
      analyst_notes: ''
    });
  }

  return dedupRawRecords_(rows);
}

// ============================================================
// SECTION 6: RAW MODEL / STORAGE
// ============================================================

function buildRawRecord_(partial) {
  const fetchedAt = isoNow_();
  const record = {
    raw_key: partial.raw_key || makeKey_([
      partial.source_name,
      partial.source_entity_id,
      partial.country_code,
      partial.film_title_raw,
      partial.record_granularity,
      partial.record_semantics,
      partial.period_key,
      partial.period_gross_local,
      partial.cumulative_gross_local
    ]),
    run_id: '',
    fetched_at: fetchedAt,
    source_name: partial.source_name || '',
    source_url: partial.source_url || '',
    parser_version: CONFIG.VERSION,
    parser_confidence: partial.parser_confidence || 0,
    source_entity_id: partial.source_entity_id || '',
    country: partial.country || '',
    country_code: partial.country_code || '',
    film_title_raw: partial.film_title_raw || '',
    film_title_ar_raw: partial.film_title_ar_raw || '',
    release_year_hint: partial.release_year_hint || '',
    record_scope: partial.record_scope || '',
    record_granularity: partial.record_granularity || '',
    record_semantics: partial.record_semantics || '',
    source_confidence: partial.source_confidence || 0,
    match_confidence: partial.match_confidence || 0,
    freshness_status: computeFreshnessStatus_(partial.source_name, fetchedAt),
    evidence_type: partial.evidence_type || '',
    period_label_raw: partial.period_label_raw || '',
    period_start_date: partial.period_start_date || '',
    period_end_date: partial.period_end_date || '',
    period_key: partial.period_key || '',
    rank: partial.rank === 0 ? 0 : (partial.rank || ''),
    period_gross_local: partial.period_gross_local || '',
    cumulative_gross_local: partial.cumulative_gross_local || '',
    currency: partial.currency || '',
    admissions_actual: partial.admissions_actual || '',
    work_id: partial.work_id || '',
    distributor: partial.distributor || '',
    notes: partial.notes || '',
    raw_payload_json: partial.raw_payload_json || '{}'
  };
  return record;
}

function appendRawEvidence_(ss, records, runId) {
  const sheet = ss.getSheetByName(CONFIG.SHEETS.RAW);
  const existing = readSheetObjects_(sheet);
  const seen = {};
  existing.forEach(r => { seen[r.raw_key] = true; });

  const toWrite = [];
  let skipped = 0;
  records.forEach(r => {
    r.run_id = runId;
    if (seen[r.raw_key]) {
      skipped++;
      return;
    }
    seen[r.raw_key] = true;
    toWrite.push(r);
  });

  appendObjects_(sheet, SCHEMAS.RAW, toWrite);
  return { added: toWrite.length, skipped: skipped };
}

function dedupRawRecords_(rows) {
  const out = [];
  const seen = {};
  rows.forEach(r => {
    if (!seen[r.raw_key]) {
      seen[r.raw_key] = true;
      out.push(r);
    }
  });
  return out;
}

// ============================================================
// SECTION 7: TITLE IDENTITY / MATCHING
// ============================================================

function resolveFilmIdForRaw_(ss, raw, ctx) {
  if (raw.record_scope !== 'title') return '';

  const films = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.FILMS));
  const aliases = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.ALIASES));

  const normalizedEn = normalizeLatinTitle_(raw.film_title_raw);
  const normalizedAr = normalizeArabicTitle_(raw.film_title_ar_raw || '');
  const yearHint = parseIntSafe_(raw.release_year_hint);

  // 1) Exact alias match
  const aliasExact = aliases.find(a => a.normalized_alias && (a.normalized_alias === normalizedEn || (normalizedAr && a.normalized_alias === normalizedAr)));
  if (aliasExact) return aliasExact.film_id;

  // 2) Candidate scoring
  const scored = [];
  films.forEach(film => {
    const canonicalEn = normalizeLatinTitle_(film.canonical_title);
    const canonicalAr = normalizeArabicTitle_(film.canonical_title_ar || '');
    const filmYear = parseIntSafe_(film.release_year);

    let score = 0;
    score = Math.max(score, titleSimilarity_(normalizedEn, canonicalEn));
    if (normalizedAr && canonicalAr) score = Math.max(score, titleSimilarity_(normalizedAr, canonicalAr));

    aliases.filter(a => a.film_id === film.film_id).forEach(alias => {
      const s = isArabicLike_(alias.alias_text) ? titleSimilarity_(normalizedAr, alias.normalized_alias) : titleSimilarity_(normalizedEn, alias.normalized_alias);
      if (s > score) score = s;
    });

    if (yearHint && filmYear) {
      if (yearHint === filmYear) score += CONFIG.MATCHING.YEAR_BONUS;
      else if (Math.abs(yearHint - filmYear) > 1) score -= CONFIG.MATCHING.YEAR_PENALTY;
    }

    if (score > 0) scored.push({ film_id: film.film_id, score: Math.max(0, Math.min(1, score)), film: film });
  });

  scored.sort((a, b) => b.score - a.score);

  if (scored.length && scored[0].score >= CONFIG.MATCHING.AUTO_MATCH_THRESHOLD) {
    return scored[0].film_id;
  }

  if (scored.length && scored[0].score >= CONFIG.MATCHING.REVIEW_THRESHOLD) {
    enqueueReview_(ss, {
      review_type: 'uncertain_film_match',
      severity: 'high',
      film_title_raw: raw.film_title_raw,
      film_id_candidate: scored[0].film_id,
      country: raw.country_code,
      period_key: raw.period_key,
      source_name: raw.source_name,
      source_url: raw.source_url,
      details_json: JSON.stringify({
        incoming_title: raw.film_title_raw,
        incoming_year: raw.release_year_hint,
        candidates: scored.slice(0, CONFIG.MATCHING.MAX_REVIEW_CANDIDATES)
      }),
      analyst_notes: ''
    });
    ctx.reviewItemsAdded++;
  }

  // 3) Create new film for strong title-performance sources only.
  const strongSource = raw.source_name === 'ELCINEMA_TITLE_BOXOFFICE' || raw.source_name === 'ELCINEMA_CHART';
  const filmId = `film_${Utilities.getUuid().slice(0, 8)}`;
  const newFilm = {
    film_id: filmId,
    canonical_title: raw.film_title_raw,
    canonical_title_ar: raw.film_title_ar_raw || '',
    release_year: raw.release_year_hint || '',
    identity_confidence: strongSource ? 'auto' : 'needs_review',
    metadata_source: raw.source_name,
    created_at: isoNow_(),
    updated_at: isoNow_()
  };
  appendObjects_(ss.getSheetByName(CONFIG.SHEETS.FILMS), SCHEMAS.FILMS, [newFilm]);
  upsertAlias_(ss, filmId, raw.film_title_raw, guessLanguage_(raw.film_title_raw), 'primary', strongSource ? 'high' : 'medium', raw.source_name, !strongSource);
  if (raw.film_title_ar_raw) upsertAlias_(ss, filmId, raw.film_title_ar_raw, 'ar', 'alternate', 'medium', raw.source_name, !strongSource);
  if (!strongSource) {
    enqueueReview_(ss, {
      review_type: 'new_film_needs_review',
      severity: 'medium',
      film_title_raw: raw.film_title_raw,
      film_id_candidate: filmId,
      country: raw.country_code,
      period_key: raw.period_key,
      source_name: raw.source_name,
      source_url: raw.source_url,
      details_json: JSON.stringify({ reason: 'Created from weak source evidence', raw_key: raw.raw_key }),
      analyst_notes: ''
    });
    ctx.reviewItemsAdded++;
  }
  return filmId;
}

function upsertAlias_(ss, filmId, aliasText, aliasLanguage, aliasType, confidence, source, needsReview) {
  if (!aliasText) return;
  const sheet = ss.getSheetByName(CONFIG.SHEETS.ALIASES);
  const rows = readSheetObjects_(sheet);
  const normalized = aliasLanguage === 'ar' ? normalizeArabicTitle_(aliasText) : normalizeLatinTitle_(aliasText);
  if (!normalized) return;
  const exists = rows.some(r => r.film_id === filmId && r.normalized_alias === normalized);
  if (exists) return;
  appendObjects_(sheet, SCHEMAS.ALIASES, [{
    alias_id: `alias_${Utilities.getUuid().slice(0, 8)}`,
    film_id: filmId,
    alias_text: aliasText,
    normalized_alias: normalized,
    alias_language: aliasLanguage,
    alias_type: aliasType,
    confidence: confidence,
    source: source,
    needs_review: needsReview ? 'yes' : 'no',
    created_at: isoNow_()
  }]);
}

// ============================================================
// SECTION 8: RECONCILIATION
// ============================================================

function reconcileEvidence_(ss, rawRows, ctx) {
  const reconGroups = {};

  rawRows.forEach(raw => {
    const filmId = resolveFilmIdForRaw_(ss, raw, ctx);
    const film = filmId ? findFilmById_(ss, filmId) : null;

    const businessKey = makeKey_([
      filmId || raw.film_title_raw,
      raw.country_code,
      raw.record_scope,
      raw.record_granularity,
      raw.record_semantics,
      raw.period_start_date,
      raw.period_end_date,
      raw.currency
    ]);

    if (!reconGroups[businessKey]) reconGroups[businessKey] = [];
    reconGroups[businessKey].push({ raw: raw, filmId: filmId, film: film });
  });

  const out = [];
  Object.keys(reconGroups).forEach(key => {
    const items = reconGroups[key];
    const comparable = items.every(i => i.raw.record_scope === items[0].raw.record_scope && i.raw.record_semantics === items[0].raw.record_semantics && i.raw.record_granularity === items[0].raw.record_granularity);
    if (!comparable) {
      items.forEach(i => enqueueConflictReview_(ss, i.raw, items.map(x => x.raw), 'Incomparable records landed in same group'));
      ctx.reviewItemsAdded += 1;
      return;
    }

    items.sort((a, b) => sourcePriorityScore_(b.raw) - sourcePriorityScore_(a.raw));
    const winner = items[0];
    const alternates = items.slice(1).map(x => x.raw.raw_key);

    // Conflict review when materially different amounts exist.
    if (items.length > 1) {
      const values = items.map(i => Number(i.raw.period_gross_local || i.raw.cumulative_gross_local || 0)).filter(Boolean);
      if (values.length >= 2) {
        const minV = Math.min.apply(null, values);
        const maxV = Math.max.apply(null, values);
        if (minV > 0 && ((maxV - minV) / minV) > 0.10) {
          enqueueConflictReview_(ss, winner.raw, items.map(x => x.raw), 'Material source disagreement > 10%');
          ctx.reviewItemsAdded += 1;
        }
      }
    }

    const film = winner.film || { canonical_title: winner.raw.film_title_raw, canonical_title_ar: '', release_year: winner.raw.release_year_hint };
    const usdPeriod = convertToUsd_(winner.raw.period_gross_local, winner.raw.currency);
    const usdCume = convertToUsd_(winner.raw.cumulative_gross_local, winner.raw.currency);
    const ticketEstimate = computeApproxTicketEstimate_(ss, winner.raw);

    out.push({
      recon_key: key,
      film_id: winner.filmId || '',
      canonical_title: film.canonical_title || winner.raw.film_title_raw,
      canonical_title_ar: film.canonical_title_ar || '',
      release_year: film.release_year || winner.raw.release_year_hint || '',
      country: winner.raw.country,
      country_code: winner.raw.country_code,
      record_scope: winner.raw.record_scope,
      record_granularity: winner.raw.record_granularity,
      record_semantics: winner.raw.record_semantics,
      evidence_type: winner.raw.evidence_type,
      period_label_raw: winner.raw.period_label_raw,
      period_start_date: winner.raw.period_start_date,
      period_end_date: winner.raw.period_end_date,
      period_key: winner.raw.period_key,
      rank: winner.raw.rank,
      period_gross_local: winner.raw.period_gross_local,
      period_gross_usd: usdPeriod,
      cumulative_gross_local: winner.raw.cumulative_gross_local,
      cumulative_gross_usd: usdCume,
      currency: winner.raw.currency,
      approx_ticket_estimate: ticketEstimate.value,
      ticket_estimate_confidence: ticketEstimate.confidence,
      source_name: winner.raw.source_name,
      source_precedence: CONFIG.SOURCE_PRECEDENCE[winner.raw.source_name] || 0,
      source_confidence: winner.raw.source_confidence,
      match_confidence: winner.raw.match_confidence,
      freshness_status: computeFreshnessStatus_(winner.raw.source_name, winner.raw.fetched_at),
      parser_confidence: winner.raw.parser_confidence,
      winning_raw_key: winner.raw.raw_key,
      alternate_raw_keys_json: JSON.stringify(alternates),
      source_url: winner.raw.source_url,
      notes: buildReconNotes_(winner.raw, alternates, ticketEstimate),
      reconciled_at: isoNow_()
    });
  });

  out.sort((a, b) => {
    const aT = a.canonical_title || '';
    const bT = b.canonical_title || '';
    return String(aT).localeCompare(String(bT)) || String(a.country_code || '').localeCompare(String(b.country_code || '')) || String(a.period_start_date || '').localeCompare(String(b.period_start_date || ''));
  });
  return out;
}

function sourcePriorityScore_(raw) {
  const precedence = CONFIG.SOURCE_PRECEDENCE[raw.source_name] || 0;
  return precedence + Number(raw.source_confidence || 0) + Number(raw.parser_confidence || 0);
}

function computeApproxTicketEstimate_(ss, raw) {
  if (raw.record_scope !== 'title' || raw.record_semantics !== 'title_period_gross' || !raw.period_gross_local || !raw.currency) {
    return { value: '', confidence: 'not_applicable' };
  }
  const ticketRows = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.TICKET_PRICES));
  const tp = ticketRows.find(r => r.country_code === raw.country_code && r.currency === raw.currency);
  if (!tp) return { value: '', confidence: 'missing' };
  if (tp.confidence !== 'high' && tp.confidence !== 'medium') return { value: '', confidence: tp.confidence || 'low' };
  const value = Math.round(Number(raw.period_gross_local) / Number(tp.avg_ticket_price_local));
  return { value: value, confidence: tp.confidence };
}

function buildReconNotes_(raw, alternates, ticketEstimate) {
  const notes = [];
  if (raw.record_semantics === 'market_chart_topline') notes.push('Market-level chart signal only; not title-level performance.');
  if (raw.record_semantics === 'title_cumulative_total') notes.push('Cumulative total only; not period gross.');
  if (ticketEstimate.value) notes.push(`Approx ticket estimate shown with ${ticketEstimate.confidence} confidence.`);
  if (alternates.length) notes.push(`${alternates.length} alternate source row(s) retained for audit.`);
  return notes.join(' ');
}

function enqueueConflictReview_(ss, winnerRaw, rawItems, message) {
  enqueueReview_(ss, {
    review_type: 'source_conflict',
    severity: 'high',
    film_title_raw: winnerRaw.film_title_raw,
    film_id_candidate: '',
    country: winnerRaw.country_code,
    period_key: winnerRaw.period_key,
    source_name: winnerRaw.source_name,
    source_url: winnerRaw.source_url,
    details_json: JSON.stringify({ message: message, raw_keys: rawItems.map(r => r.raw_key) }),
    analyst_notes: ''
  });
}

// ============================================================
// SECTION 9: LOOKUP / ACQUISITION REPORT
// ============================================================

function renderLookup_(query, forceLive) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  const sheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  setupLookupSheet_(ss, query);
  clearLookupOutput_(sheet);
  sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).setValue(query);

  const recon = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RECON));
  const films = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.FILMS));
  const aliases = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.ALIASES));

  const match = matchQueryToFilm_(query, films, aliases);
  let relevant = [];
  const normQueryEn = normalizeLatinTitle_(query);
  const normQueryAr = normalizeArabicTitle_(query);

  if (match.film_id) {
    relevant = recon.filter(r => r.film_id === match.film_id);
  } else {
    relevant = recon.filter(r => {
      const t1 = normalizeLatinTitle_(r.canonical_title || r.film_title_raw || '');
      const t2 = normalizeArabicTitle_(r.canonical_title_ar || r.film_title_ar_raw || '');
      return (t1 && (t1.indexOf(normQueryEn) >= 0 || normQueryEn.indexOf(t1) >= 0 || titleSimilarity_(normQueryEn, t1) >= 0.75)) ||
             (t2 && (t2.indexOf(normQueryAr) >= 0 || normQueryAr.indexOf(t2) >= 0 || titleSimilarity_(normQueryAr, t2) >= 0.80));
    });
  }

  const live = forceLive ? fetchLiveLookupSignals_(query, match) : { rows: [], status: [] };
  const allRows = dedupReconLikeRows_(relevant.concat(live.rows));

  const warnings = [];
  if (!allRows.length) warnings.push('No local or live evidence found for this query. Current-chart checks and title-page discovery both returned no usable evidence.');
  const staleMarkets = unique_(allRows.filter(r => r.freshness_status === 'stale').map(r => r.country_code));
  if (staleMarkets.length) warnings.push(`Stale evidence exists in: ${staleMarkets.join(', ')}`);
  const chartOnlyMarkets = unique_(allRows.filter(r => r.record_semantics === 'market_chart_topline').map(r => r.country_code));
  if (chartOnlyMarkets.length) warnings.push(`Chart-only markets (not title tracking): ${chartOnlyMarkets.join(', ')}`);
  if (forceLive && !allRows.some(r => r.record_scope === 'title')) warnings.push('No title-level live performance evidence was found. Any chart signals below should not be used as standalone acquisition proof.');

  const startRow = 6;

  const overview = [
    ['Matched Film', match.title || query, 'Match Confidence', match.score || ''],
    ['Film ID', match.film_id || '', 'Identity Status', match.identity_confidence || (match.film_id ? '' : 'unmatched')],
    ['Release Year', match.release_year || '', 'Live Refresh', forceLive ? isoNow_() : 'No']
  ];
  sheet.getRange(startRow, 1, overview.length, 4).setValues(overview);
  sheet.getRange(startRow, 1, 1, 4).setFontWeight('bold').setBackground('#E5E7EB');

  const warnRow = startRow + 4;
  sheet.getRange(warnRow, 1).setValue('Acquisition Warnings').setFontWeight('bold').setBackground('#FDE68A');
  const warnValues = warnings.length ? warnings.map(w => [w]) : [['No critical warnings generated.']];
  sheet.getRange(warnRow + 1, 1, warnValues.length, 1).setValues(warnValues);

  const coverageRow = warnRow + warnValues.length + 3;
  sheet.getRange(coverageRow, 1).setValue('Market Coverage Summary').setFontWeight('bold').setBackground('#DBEAFE');
  const coverage = buildMarketCoverage_(allRows);
  const coverageHeader = [['Country', 'Strongest Evidence', 'Latest Period', 'Latest Gross', 'Latest Total', 'Weeks/Records', 'Freshness', 'Source']];
  sheet.getRange(coverageRow + 1, 1, 1, coverageHeader[0].length).setValues(coverageHeader).setFontWeight('bold');
  if (coverage.length) sheet.getRange(coverageRow + 2, 1, coverage.length, coverageHeader[0].length).setValues(coverage);

  const perfRow = coverageRow + 3 + Math.max(coverage.length, 1);
  sheet.getRange(perfRow, 1).setValue('Comparable Title Performance Evidence').setFontWeight('bold').setBackground('#D1FAE5');
  const perfHeader = [['Country', 'Granularity', 'Period', 'Period Gross (Local)', 'Currency', 'Cumulative (Local)', 'Approx Tickets', 'Ticket Confidence', 'Source', 'Freshness']];
  sheet.getRange(perfRow + 1, 1, 1, perfHeader[0].length).setValues(perfHeader).setFontWeight('bold');
  const perfRows = allRows
    .filter(r => r.record_scope === 'title' && (r.record_semantics === 'title_period_gross' || r.record_semantics === 'title_cumulative_total'))
    .sort(sortReconRowsForLookup_)
    .map(r => [r.country, r.record_granularity, r.period_key || r.period_label_raw || 'lifetime', r.period_gross_local || '', r.currency || '', r.cumulative_gross_local || '', r.approx_ticket_estimate || '', r.ticket_estimate_confidence || '', r.source_name, r.freshness_status]);
  if (perfRows.length) sheet.getRange(perfRow + 2, 1, perfRows.length, perfHeader[0].length).setValues(perfRows);
  else sheet.getRange(perfRow + 2, 1).setValue('No title-level comparable evidence available.');

  const chartRow = perfRow + 4 + Math.max(perfRows.length, 1);
  sheet.getRange(chartRow, 1).setValue('Chart Mentions / Market-Level Signals (Do Not Treat as Title Performance)').setFontWeight('bold').setBackground('#FECACA');
  const chartHeader = [['Country', 'Period', 'Top Title Mentioned', 'Market Weekend Gross', 'Currency', 'Source', 'Freshness']];
  sheet.getRange(chartRow + 1, 1, 1, chartHeader[0].length).setValues(chartHeader).setFontWeight('bold');
  const chartRows = allRows
    .filter(r => r.record_semantics === 'market_chart_topline')
    .sort(sortReconRowsForLookup_)
    .map(r => [r.country, r.period_key || r.period_label_raw, r.canonical_title || r.film_title_raw || '', r.period_gross_local || '', r.currency || '', r.source_name, r.freshness_status]);
  if (chartRows.length) sheet.getRange(chartRow + 2, 1, chartRows.length, chartHeader[0].length).setValues(chartRows);
  else sheet.getRange(chartRow + 2, 1).setValue('No chart-mention rows found.');

  const statusRow = chartRow + 4 + Math.max(chartRows.length, 1);
  sheet.getRange(statusRow, 1).setValue('Live Source Status').setFontWeight('bold').setBackground('#E9D5FF');
  const statusHeader = [['Source', 'Status', 'Rows', 'Notes']];
  sheet.getRange(statusRow + 1, 1, 1, 4).setValues(statusHeader).setFontWeight('bold');
  const statuses = live.status.length ? live.status : [{ source: 'local_only', status: 'Not refreshed live', rows: '', notes: 'Used local reconciled data only' }];
  const statusValues = statuses.map(s => [s.source, s.status, s.rows, s.notes]);
  sheet.getRange(statusRow + 2, 1, statusValues.length, 4).setValues(statusValues);

  autoResize_(sheet, 12);
  renderDashboard_(query, inferBestDashboardCountry_(allRows));
}

function inferBestDashboardCountry_(rows) {
  const counts = {};
  rows.filter(r => r.record_scope === 'title' && r.record_semantics === 'title_period_gross').forEach(r => {
    counts[r.country_code] = (counts[r.country_code] || 0) + 1;
  });
  let best = 'ALL';
  let max = 0;
  Object.keys(counts).forEach(code => {
    if (counts[code] > max) {
      best = code;
      max = counts[code];
    }
  });
  return best;
}

function buildMarketCoverage_(rows) {
  const byMarket = {};
  rows.forEach(r => {
    if (!byMarket[r.country_code]) byMarket[r.country_code] = [];
    byMarket[r.country_code].push(r);
  });
  const out = [];
  Object.keys(CONFIG.MARKETS).forEach(code => {
    const list = byMarket[code] || [];
    if (!list.length) {
      out.push([CONFIG.MARKETS[code].country, 'No evidence', '', '', '', 0, '', '']);
      return;
    }
    const perf = list.filter(r => r.record_scope === 'title' && r.record_semantics === 'title_period_gross');
    const strongest = perf.length ? 'Title weekly performance' : (list.some(r => r.record_semantics === 'title_cumulative_total') ? 'Cumulative total only' : 'Chart mention only');
    const latest = list.slice().sort(sortReconRowsForLookup_)[0];
    out.push([
      latest.country,
      strongest,
      latest.period_key || latest.period_label_raw || '',
      latest.period_gross_local || '',
      latest.cumulative_gross_local || '',
      list.length,
      latest.freshness_status,
      latest.source_name
    ]);
  });
  return out;
}

function fetchLiveLookupSignals_(query, match) {
  const status = [];
  let rows = [];
  const matchedWorkIds = {};

  try {
    const intl = fetchBoxOfficeMojoIntlLatest_().filter(r => titleMatchesQuery_(query, r.film_title_raw, '', r.release_year_hint, match && match.release_year));
    rows = rows.concat(intl);
    status.push({ source: 'BOXOFFICEMOJO_INTL', status: 'OK', rows: intl.length, notes: 'Latest international weekend chart signal' });
  } catch (e) {
    status.push({ source: 'BOXOFFICEMOJO_INTL', status: 'ERROR', rows: 0, notes: e.message });
  }

  const currentYear = new Date().getFullYear();
  Object.keys(CONFIG.MARKETS).forEach(code => {
    let marketRows = [];
    try {
      for (let offset = 0; offset < Math.max(CONFIG.PIPELINE.FETCH_MOJO_AREA_YEARS + 1, 3); offset++) {
        const year = currentYear - offset;
        const yrRows = fetchBoxOfficeMojoAreaYear_(code, year).filter(r => titleMatchesQuery_(query, r.film_title_raw, '', r.release_year_hint, match && match.release_year));
        marketRows = marketRows.concat(yrRows);
      }
      rows = rows.concat(marketRows);
      status.push({ source: `BOXOFFICEMOJO_AREA_YEAR:${code}`, status: 'OK', rows: marketRows.length, notes: marketRows.length ? 'Historical market weekend chart hits found' : 'No historical chart hits found' });
    } catch (e) {
      status.push({ source: `BOXOFFICEMOJO_AREA_YEAR:${code}`, status: 'ERROR', rows: 0, notes: e.message });
    }
  });

  try {
    const chart = fetchElCinemaCurrentChart_().filter(r => titleMatchesQuery_(query, r.film_title_raw, '', r.release_year_hint, match && match.release_year));
    chart.forEach(r => { if (r.work_id) matchedWorkIds[r.work_id] = true; });
    rows = rows.concat(chart);
    status.push({ source: 'ELCINEMA_CHART', status: 'OK', rows: chart.length, notes: 'Current Egypt chart title hits' });
  } catch (e) {
    status.push({ source: 'ELCINEMA_CHART', status: 'ERROR', rows: 0, notes: e.message });
  }

  try {
    const discovered = discoverElCinemaWorkCandidates_(query, match && match.release_year);
    discovered.forEach(c => { if (c.work_id) matchedWorkIds[c.work_id] = true; });
    status.push({ source: 'ELCINEMA_DISCOVERY', status: 'OK', rows: discovered.length, notes: discovered.length ? 'Search/index discovery candidates found' : 'No elCinema discovery candidates found' });
  } catch (e) {
    status.push({ source: 'ELCINEMA_DISCOVERY', status: 'ERROR', rows: 0, notes: e.message });
  }

  Object.keys(matchedWorkIds).forEach(id => {
    try {
      const detail = fetchElCinemaTitleBoxOffice_(id);
      rows = rows.concat(detail);
      status.push({ source: `ELCINEMA_TITLE_BOXOFFICE:${id}`, status: 'OK', rows: detail.length, notes: 'Title history fetched' });
    } catch (e) {
      status.push({ source: `ELCINEMA_TITLE_BOXOFFICE:${id}`, status: 'ERROR', rows: 0, notes: e.message });
    }
  });

  rows = dedupRawRecords_(rows);

  const fauxRecon = rows.map(r => ({
    film_id: '',
    canonical_title: r.film_title_raw,
    canonical_title_ar: r.film_title_ar_raw || '',
    release_year: r.release_year_hint || '',
    country: r.country,
    country_code: r.country_code,
    record_scope: r.record_scope,
    record_granularity: r.record_granularity,
    record_semantics: r.record_semantics,
    evidence_type: r.evidence_type,
    period_label_raw: r.period_label_raw,
    period_start_date: r.period_start_date,
    period_end_date: r.period_end_date,
    period_key: r.period_key,
    rank: r.rank,
    period_gross_local: r.period_gross_local,
    period_gross_usd: convertToUsd_(r.period_gross_local, r.currency),
    cumulative_gross_local: r.cumulative_gross_local,
    cumulative_gross_usd: convertToUsd_(r.cumulative_gross_local, r.currency),
    currency: r.currency,
    approx_ticket_estimate: '',
    ticket_estimate_confidence: '',
    source_name: r.source_name,
    source_precedence: CONFIG.SOURCE_PRECEDENCE[r.source_name] || 0,
    source_confidence: r.source_confidence,
    match_confidence: r.match_confidence,
    freshness_status: computeFreshnessStatus_(r.source_name, r.fetched_at),
    parser_confidence: r.parser_confidence,
    winning_raw_key: r.raw_key,
    alternate_raw_keys_json: '[]',
    source_url: r.source_url,
    notes: r.notes,
    reconciled_at: isoNow_(),
    film_title_raw: r.film_title_raw,
    film_title_ar_raw: r.film_title_ar_raw || ''
  }));

  return { rows: dedupReconLikeRows_(fauxRecon), status: status };
}

function discoverElCinemaWorkCandidates_(query, releaseYearHint) {
  const candidates = [];
  const seen = {};
  const queryEnc = encodeURIComponent(query);
  const searchUrls = [
    `https://elcinema.com/en/index/work/search/?s=${queryEnc}`,
    `https://elcinema.com/en/index/work/?s=${queryEnc}`,
    `https://elcinema.com/en/boxoffice/`,
    `https://elcinema.com/en/boxoffice/EG/`,
    `https://elcinema.com/en/boxoffice/SA/`
  ];

  searchUrls.forEach(url => {
    try {
      const html = fetchUrl_(url);
      const rx = /href="\/en\/work\/(\d+)\/"[^>]*>([\s\S]*?)<\/a>/gi;
      let m;
      while ((m = rx.exec(html)) !== null) {
        const workId = m[1];
        const title = htmlToText_(m[2]).trim();
        if (!title || title.length < 2 || /^[\d\.]+$/.test(title)) continue;
        if (!titleMatchesQuery_(query, title, '', releaseYearHint, releaseYearHint)) continue;
        if (!seen[workId]) {
          seen[workId] = true;
          candidates.push({ work_id: workId, matched_title: title, source_url: url });
        }
      }
    } catch (e) {
      // ignore per-url failures during discovery
    }
  });
  return candidates;
}

function dedupReconLikeRows_(rows) {
  const seen = {};
  const out = [];
  rows.forEach(r => {
    const key = [r.source_name, r.country_code, r.record_scope, r.record_semantics, r.period_key || r.period_label_raw || '', normalizeLatinTitle_(r.canonical_title || r.film_title_raw || ''), normalizeArabicTitle_(r.canonical_title_ar || r.film_title_ar_raw || '')].join('|');
    if (!seen[key]) {
      seen[key] = true;
      out.push(r);
    }
  });
  return out;
}

function sortReconRowsForLookup_(a, b) {
  const countryCmp = String(a.country_code || '').localeCompare(String(b.country_code || ''));
  if (countryCmp !== 0) return countryCmp;
  const aDate = String(a.period_start_date || a.period_end_date || '');
  const bDate = String(b.period_start_date || b.period_end_date || '');
  const dateCmp = bDate.localeCompare(aDate);
  if (dateCmp !== 0) return dateCmp;
  return String(a.source_name || '').localeCompare(String(b.source_name || ''));
}

function titleMatchesQuery_(query, candidateEn, candidateAr, candidateYear, releaseYearHint) {
  const qEn = normalizeLatinTitle_(query);
  const qAr = normalizeArabicTitle_(query);
  const cEn = normalizeLatinTitle_(candidateEn || '');
  const cAr = normalizeArabicTitle_(candidateAr || '');
  let score = 0;

  if (qEn && cEn) {
    if (qEn === cEn) score = Math.max(score, 1);
    if (qEn.indexOf(cEn) >= 0 || cEn.indexOf(qEn) >= 0) score = Math.max(score, 0.93);
    score = Math.max(score, titleSimilarity_(qEn, cEn));
  }
  if (qAr && cAr) {
    if (qAr === cAr) score = Math.max(score, 1);
    if (qAr.indexOf(cAr) >= 0 || cAr.indexOf(qAr) >= 0) score = Math.max(score, 0.95);
    score = Math.max(score, titleSimilarity_(qAr, cAr));
  }

  const qEnCompact = qEn.replace(/\s+/g, '');
  const cEnCompact = cEn.replace(/\s+/g, '');
  if (qEnCompact && cEnCompact && (qEnCompact === cEnCompact || qEnCompact.indexOf(cEnCompact) >= 0 || cEnCompact.indexOf(qEnCompact) >= 0)) score = Math.max(score, 0.95);

  if (releaseYearHint && candidateYear && String(releaseYearHint) === String(candidateYear)) score += CONFIG.MATCHING.YEAR_BONUS;
  if (releaseYearHint && candidateYear && String(releaseYearHint) !== String(candidateYear)) score -= CONFIG.MATCHING.YEAR_PENALTY;
  return score >= 0.82;
}

function matchQueryToFilm_(query, films, aliases) {
  const normalizedEn = normalizeLatinTitle_(query);
  const normalizedAr = normalizeArabicTitle_(query);

  const candidates = [];
  films.forEach(f => {
    let score = Math.max(titleSimilarity_(normalizedEn, normalizeLatinTitle_(f.canonical_title)), titleSimilarity_(normalizedAr, normalizeArabicTitle_(f.canonical_title_ar || '')));
    aliases.filter(a => a.film_id === f.film_id).forEach(a => {
      const ns = a.alias_language === 'ar' ? normalizedAr : normalizedEn;
      score = Math.max(score, titleSimilarity_(ns, a.normalized_alias));
    });
    if (score > 0) candidates.push({ film_id: f.film_id, title: f.canonical_title, release_year: f.release_year, identity_confidence: f.identity_confidence, score: score });
  });

  candidates.sort((a, b) => b.score - a.score);
  return candidates[0] || { film_id: '', title: '', release_year: '', identity_confidence: '', score: 0 };
}

// ============================================================
// SECTION 10: REVIEW QUEUE / RUN LOG
// ============================================================

function createRunContext_() {
  return {
    runId: `run_${new Date().getTime()}`,
    startedAt: isoNow_(),
    sourceAttempts: 0,
    sourceSuccesses: 0,
    sourceFailures: 0,
    rowsFetched: 0,
    rawAdded: 0,
    rawSkipped: 0,
    reconciledWritten: 0,
    reviewItemsAdded: 0,
    parserWarnings: 0,
    anomalies: 0,
    notes: []
  };
}

function finalizeRunLog_(ss, ctx) {
  appendObjects_(ss.getSheetByName(CONFIG.SHEETS.RUN_LOG), SCHEMAS.RUN_LOG, [{
    run_id: ctx.runId,
    started_at: ctx.startedAt,
    completed_at: isoNow_(),
    pipeline_version: CONFIG.VERSION,
    source_attempts: ctx.sourceAttempts,
    source_successes: ctx.sourceSuccesses,
    source_failures: ctx.sourceFailures,
    rows_fetched: ctx.rowsFetched,
    raw_added: ctx.rawAdded,
    raw_skipped: ctx.rawSkipped,
    reconciled_written: ctx.reconciledWritten,
    review_items_added: ctx.reviewItemsAdded,
    parser_warnings: ctx.parserWarnings,
    anomalies: ctx.anomalies,
    notes: (Array.isArray(ctx.notes) ? ctx.notes : [String(ctx.notes || '')]).filter(String).join(' | ')
  }]);
}

function enqueueReview_(ss, payload) {
  const sheet = ss.getSheetByName(CONFIG.SHEETS.REVIEW);
  appendObjects_(sheet, SCHEMAS.REVIEW, [{
    review_id: `review_${Utilities.getUuid().slice(0, 8)}`,
    created_at: isoNow_(),
    review_type: payload.review_type,
    status: 'open',
    severity: payload.severity || 'medium',
    film_title_raw: payload.film_title_raw || '',
    film_id_candidate: payload.film_id_candidate || '',
    country: payload.country || '',
    period_key: payload.period_key || '',
    source_name: payload.source_name || '',
    source_url: payload.source_url || '',
    details_json: payload.details_json || '{}',
    analyst_notes: payload.analyst_notes || ''
  }]);
}

// ============================================================
// SECTION 11: HELPERS - SHEETS
// ============================================================

function appendObjects_(sheet, schema, objects) {
  if (!objects || !objects.length) return;
  const rows = objects.map(obj => schema.map(k => obj[k] !== undefined ? obj[k] : ''));
  appendRows_(sheet, rows);
}

function rewriteSheetObjects_(sheet, schema, objects) {
  ensureHeader_(sheet, schema);
  if (sheet.getLastRow() > 1) sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).clearContent();
  appendObjects_(sheet, schema, objects);
}

function appendRows_(sheet, rows) {
  if (!rows || !rows.length) return;
  const start = sheet.getLastRow() + 1;
  sheet.getRange(start, 1, rows.length, rows[0].length).setValues(rows);
}

function readSheetObjects_(sheet) {
  const values = sheet.getDataRange().getValues();
  if (!values || values.length < 2) return [];
  const headers = values[0];
  return values.slice(1).filter(r => r.join('') !== '').map(r => {
    const obj = {};
    headers.forEach((h, i) => obj[h] = r[i]);
    return obj;
  });
}

function findFilmById_(ss, filmId) {
  const films = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.FILMS));
  return films.find(f => f.film_id === filmId) || null;
}

function autoResize_(sheet, width) {
  for (let i = 1; i <= width; i++) {
    try { sheet.autoResizeColumn(i); } catch (e) {}
  }
}

// ============================================================
// SECTION 12: HELPERS - PARSING / NORMALIZATION
// ============================================================

function htmlToText_(html) {
  if (!html) return '';
  return String(html)
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<\/tr>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/div>/gi, '\n')
    .replace(/<\/li>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n[ \t]+/g, '\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{2,}/g, '\n')
    .trim();
}

function normalizeWhitespace_(text) {
  return String(text || '').replace(/\s+/g, ' ');
}

function parseMoney_(text) {
  if (text === '' || text === null || text === undefined) return '';
  const cleaned = String(text).replace(/[^0-9.\-]/g, '');
  if (!cleaned) return '';
  const n = parseFloat(cleaned);
  return isNaN(n) ? '' : n;
}

function parseIntSafe_(text) {
  const n = parseInt(String(text || '').replace(/[^0-9\-]/g, ''), 10);
  return isNaN(n) ? '' : n;
}

function toIsoDateFromLong_(label) {
  const d = new Date(label);
  if (isNaN(d.getTime())) return '';
  return Utilities.formatDate(d, CONFIG.TIMEZONE, 'yyyy-MM-dd');
}

function parseMojoWeekendLabel_(label, year) {
  if (!label) return null;
  const m = String(label).trim().match(/([A-Za-z]+)\s+(\d{1,2})-(?:([A-Za-z]+)\s+)?(\d{1,2})/);
  if (!m) return null;
  const sm = MONTH_MAP[m[1].slice(0, 3).toLowerCase()];
  const em = MONTH_MAP[(m[3] || m[1]).slice(0, 3).toLowerCase()];
  if (!sm || !em) return null;
  let sy = year;
  let ey = year;
  if (sm === 12 && em === 1) ey = year + 1;
  const start = `${sy}-${pad2_(sm)}-${pad2_(m[2])}`;
  const end = `${ey}-${pad2_(em)}-${pad2_(m[4])}`;
  return { start: start, end: end, key: start };
}

function parseIsoWeek_(label) {
  const m = String(label || '').match(/(20\d{2})-W(\d{1,2})/);
  if (!m) return null;
  const year = parseInt(m[1], 10);
  const week = parseInt(m[2], 10);
  const jan4 = new Date(Date.UTC(year, 0, 4));
  const jan4Day = jan4.getUTCDay() || 7;
  const monday = new Date(jan4);
  monday.setUTCDate(jan4.getUTCDate() - jan4Day + 1 + (week - 1) * 7);
  const sunday = new Date(monday);
  sunday.setUTCDate(monday.getUTCDate() + 6);
  return {
    start: Utilities.formatDate(monday, 'UTC', 'yyyy-MM-dd'),
    end: Utilities.formatDate(sunday, 'UTC', 'yyyy-MM-dd'),
    key: `${year}-W${pad2_(week)}`
  };
}

function detectElCinemaWeekLabel_(html) {
  const text = htmlToText_(html);
  let m = text.match(/year\s+(20\d{2})\s+week\s+(\d{1,2})/i);
  if (m) return `${m[1]}-W${pad2_(m[2])}`;
  m = text.match(/Egyptian Boxoffice year\s+(20\d{2})\s+week\s+(\d{1,2})/i);
  if (m) return `${m[1]}-W${pad2_(m[2])}`;
  return '';
}

function normalizeLatinTitle_(text) {
  text = String(text || '').toLowerCase();
  text = text.normalize ? text.normalize('NFD').replace(/[\u0300-\u036f]/g, '') : text;
  text = text.replace(/&/g, ' and ');
  text = text.replace(/\b(the|a|an)\b/g, ' ');
  text = text.replace(/[^a-z0-9\s]/g, ' ');
  text = text.replace(/\s+/g, ' ').trim();
  return text;
}

function normalizeArabicTitle_(text) {
  text = String(text || '').trim();
  text = text.replace(/[\u064B-\u065F\u0670]/g, '');
  text = text.replace(/[إأآا]/g, 'ا');
  text = text.replace(/ى/g, 'ي');
  text = text.replace(/ة/g, 'ه');
  text = text.replace(/ؤ/g, 'و');
  text = text.replace(/ئ/g, 'ي');
  text = text.replace(/ـ/g, '');
  text = text.replace(/[^\u0600-\u06FF0-9\s]/g, ' ');
  text = text.replace(/\s+/g, ' ').trim();
  return text;
}

function titleSimilarity_(a, b) {
  a = String(a || '').trim();
  b = String(b || '').trim();
  if (!a || !b) return 0;
  if (a === b) return 1;
  if (a.indexOf(b) >= 0 || b.indexOf(a) >= 0) return 0.92;
  const aTokens = a.split(' ').filter(Boolean);
  const bTokens = b.split(' ').filter(Boolean);
  if (!aTokens.length || !bTokens.length) return 0;
  const inter = aTokens.filter(t => bTokens.indexOf(t) >= 0).length;
  const union = unique_(aTokens.concat(bTokens)).length;
  const jaccard = inter / union;
  const prefix = (a.slice(0, 8) === b.slice(0, 8)) ? 0.08 : 0;
  return Math.max(0, Math.min(1, jaccard + prefix));
}

function guessLanguage_(text) {
  return isArabicLike_(text) ? 'ar' : 'en';
}

function isArabicLike_(text) {
  return /[\u0600-\u06FF]/.test(String(text || ''));
}

function findMarketCodeByCountry_(countryLabel) {
  const t = normalizeLatinTitle_(countryLabel);
  const exact = Object.keys(CONFIG.MARKETS).find(code => normalizeLatinTitle_(CONFIG.MARKETS[code].country) === t);
  if (exact) return exact;
  const aliases = {
    uae: 'AE', 'united arab emirates': 'AE', egypt: 'EG', 'saudi arabia': 'SA', saudi: 'SA', kuwait: 'KW',
    bahrain: 'BH', lebanon: 'LB', qatar: 'QA', oman: 'OM', jordan: 'JO'
  };
  return aliases[t] || '';
}

function computeFreshnessStatus_(sourceName, fetchedAt) {
  const limit = CONFIG.FRESHNESS_DAYS[sourceName] || 14;
  if (!fetchedAt) return 'unknown';
  const ageDays = (new Date().getTime() - new Date(fetchedAt).getTime()) / (1000 * 60 * 60 * 24);
  return ageDays <= limit ? 'fresh' : 'stale';
}

function convertToUsd_(value, currency) {
  if (value === '' || value === null || value === undefined) return '';
  const fx = CONFIG.FX_TO_USD[String(currency || '').toUpperCase()] || '';
  if (!fx) return '';
  return Math.round(Number(value) * fx * 100) / 100;
}

// ============================================================
// SECTION 13: HELPERS - GENERIC
// ============================================================

function makeKey_(parts) {
  return parts.map(p => String(p || '').trim().toLowerCase()).join('|').replace(/\s+/g, ' ');
}

function pad2_(n) {
  return ('0' + n).slice(-2);
}

function isoNow_() {
  return new Date().toISOString();
}

function unique_(arr) {
  const seen = {};
  const out = [];
  arr.forEach(v => {
    const k = String(v);
    if (!seen[k]) {
      seen[k] = true;
      out.push(v);
    }
  });
  return out;
}


// ============================================================
// SECTION 14: v4.2 PATCH OVERRIDES - TITLE RESOLUTION FIRST
// ============================================================

/**
 * v4.2 patch goals:
 *  - stop treating current charts as the primary lookup path
 *  - resolve title pages first (elCinema + Box Office Mojo)
 *  - fetch title-specific evidence second
 *  - use chart pages only as supplemental market signals
 *  - preserve lookup input state
 */

function setupLookupSheet_(ss, preserveQuery) {
  var sheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  if (!sheet) sheet = ss.insertSheet(CONFIG.SHEETS.LOOKUP);

  var existingQuery = preserveQuery !== undefined
    ? preserveQuery
    : String(sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '');

  // Build layout without wiping analyst input/output unless this is a fresh sheet.
  if (!String(sheet.getRange('A1').getValue() || '').trim()) {
    sheet.getRange('A1').setValue('MENA Box Office Acquisition Lookup');
  }
  sheet.getRange('A1').setValue('MENA Box Office Acquisition Lookup').setFontSize(15).setFontWeight('bold');
  sheet.getRange('A2').setValue('Title Query').setFontWeight('bold');
  sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL)
    .setBackground('#FFF9C4')
    .setFontWeight('bold')
    .setNote('Enter a film title, then use menu: Search Title or Refresh Live Lookup');
  if (existingQuery) sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).setValue(existingQuery);
  sheet.getRange('A4').setValue('Use this page for acquisition research.').setFontColor('#6B7280');
  sheet.setFrozenRows(4);
  sheet.setColumnWidths(1, 12, 150);
  sheet.setColumnWidth(2, 280);
}

function fetchLiveLookupSignals_(query, match) {
  var status = [];
  var rows = [];
  var yearHint = extractYearHintFromQuery_(query) || (match && match.release_year) || '';
  var discoveredWorkIds = {};
  var discoveredMojoReleaseUrls = {};
  var discoveredMojoTitleUrls = {};

  // 1) elCinema title discovery first: search engines + direct weak fallbacks
  try {
    var eCandidates = discoverElCinemaWorkCandidates_(query, yearHint);
    status.push({
      source: 'ELCINEMA_DISCOVERY',
      status: 'OK',
      rows: eCandidates.length,
      notes: eCandidates.length ? 'Resolved elCinema work candidates from title discovery' : 'No elCinema title candidates resolved'
    });
    eCandidates.forEach(function(c) { if (c.work_id) discoveredWorkIds[c.work_id] = c; });
  } catch (e) {
    status.push({ source: 'ELCINEMA_DISCOVERY', status: 'ERROR', rows: 0, notes: e.message });
  }

  // 2) Pull title-specific elCinema weekly history for every resolved work
  Object.keys(discoveredWorkIds).slice(0, 5).forEach(function(workId) {
    try {
      var detail = fetchElCinemaTitleBoxOffice_(workId);
      // Harden match confidence using resolved title metadata if available.
      detail = detail.filter(function(r) {
        return titleMatchesQuery_(query, r.film_title_raw, r.film_title_ar_raw || '', r.release_year_hint, yearHint);
      }).map(function(r) {
        r.match_confidence = Math.max(Number(r.match_confidence || 0), 0.90);
        return r;
      });
      rows = rows.concat(detail);
      status.push({
        source: 'ELCINEMA_TITLE_BOXOFFICE:' + workId,
        status: 'OK',
        rows: detail.length,
        notes: detail.length ? 'Fetched title-specific weekly history' : 'No weekly history rows on title box office page'
      });
    } catch (e) {
      status.push({ source: 'ELCINEMA_TITLE_BOXOFFICE:' + workId, status: 'ERROR', rows: 0, notes: e.message });
    }

    try {
      var released = fetchElCinemaReleasedMarkets_(workId);
      status.push({
        source: 'ELCINEMA_RELEASED:' + workId,
        status: 'OK',
        rows: released.length,
        notes: released.length ? ('Release dates found for: ' + released.map(function(x){ return x.country_code; }).join(', ')) : 'No release-date markets found'
      });
    } catch (e) {
      status.push({ source: 'ELCINEMA_RELEASED:' + workId, status: 'ERROR', rows: 0, notes: e.message });
    }
  });

  // 3) Current chart remains supplemental only
  try {
    var chart = fetchElCinemaCurrentChart_().filter(function(r) {
      return titleMatchesQuery_(query, r.film_title_raw, r.film_title_ar_raw || '', r.release_year_hint, yearHint);
    }).map(function(r) {
      r.match_confidence = Math.max(Number(r.match_confidence || 0), 0.80);
      return r;
    });
    rows = rows.concat(chart);
    status.push({ source: 'ELCINEMA_CHART', status: 'OK', rows: chart.length, notes: 'Current Egypt chart title hits (supplemental only)' });
  } catch (e) {
    status.push({ source: 'ELCINEMA_CHART', status: 'ERROR', rows: 0, notes: e.message });
  }

  // 4) Resolve Box Office Mojo title/release pages via search engine discovery
  try {
    var mCandidates = discoverBoxOfficeMojoCandidates_(query, yearHint);
    status.push({
      source: 'BOXOFFICEMOJO_DISCOVERY',
      status: 'OK',
      rows: mCandidates.length,
      notes: mCandidates.length ? 'Resolved Box Office Mojo title/release candidates' : 'No Box Office Mojo title/release candidates resolved'
    });
    mCandidates.forEach(function(c) {
      if (c.release_url) discoveredMojoReleaseUrls[c.release_url] = c;
      if (c.title_url) discoveredMojoTitleUrls[c.title_url] = c;
    });
  } catch (e) {
    status.push({ source: 'BOXOFFICEMOJO_DISCOVERY', status: 'ERROR', rows: 0, notes: e.message });
  }

  // 5) Territory-specific release pages are the strongest BOM title evidence in this workbook.
  Object.keys(discoveredMojoReleaseUrls).slice(0, 6).forEach(function(url) {
    try {
      var relRows = fetchBoxOfficeMojoReleaseEvidence_(url, query, yearHint);
      rows = rows.concat(relRows);
      status.push({
        source: 'BOXOFFICEMOJO_RELEASE',
        status: 'OK',
        rows: relRows.length,
        notes: relRows.length ? 'Fetched territory-specific release evidence' : 'Release page resolved but yielded no usable rows'
      });
    } catch (e) {
      status.push({ source: 'BOXOFFICEMOJO_RELEASE', status: 'ERROR', rows: 0, notes: e.message });
    }
  });

  // 6) Title pages can reveal linked release pages. Use them when release URLs were not directly discovered.
  Object.keys(discoveredMojoTitleUrls).slice(0, 4).forEach(function(titleUrl) {
    try {
      var titleInfo = fetchBoxOfficeMojoTitleCandidate_(titleUrl);
      var emitted = 0;
      (titleInfo.release_urls || []).forEach(function(releaseUrl) {
        if (discoveredMojoReleaseUrls[releaseUrl]) return;
        discoveredMojoReleaseUrls[releaseUrl] = true;
        try {
          var relRows = fetchBoxOfficeMojoReleaseEvidence_(releaseUrl, query, yearHint);
          rows = rows.concat(relRows);
          emitted += relRows.length;
        } catch (e2) {}
      });

      // If we still found no release rows, emit the title-level total as weak/global evidence only if it matches.
      if (!emitted && titleInfo && titleInfo.title_en && titleMatchesQuery_(query, titleInfo.title_en, '', titleInfo.release_year, yearHint)) {
        var totalRows = buildBoxOfficeMojoTitleSummaryRows_(titleInfo);
        rows = rows.concat(totalRows);
        emitted += totalRows.length;
      }

      status.push({
        source: 'BOXOFFICEMOJO_TITLE',
        status: 'OK',
        rows: emitted,
        notes: emitted ? 'Fetched title page and/or linked release evidence' : 'Title page resolved but yielded no usable rows'
      });
    } catch (e) {
      status.push({ source: 'BOXOFFICEMOJO_TITLE', status: 'ERROR', rows: 0, notes: e.message });
    }
  });

  // 7) Current BOM intl chart remains supplemental and explicit chart-only evidence.
  try {
    var intl = fetchBoxOfficeMojoIntlLatest_().filter(function(r) {
      return titleMatchesQuery_(query, r.film_title_raw, '', r.release_year_hint, yearHint);
    }).map(function(r) {
      r.match_confidence = Math.max(Number(r.match_confidence || 0), 0.70);
      return r;
    });
    rows = rows.concat(intl);
    status.push({ source: 'BOXOFFICEMOJO_INTL', status: 'OK', rows: intl.length, notes: 'Latest international weekend chart signal (supplemental only)' });
  } catch (e) {
    status.push({ source: 'BOXOFFICEMOJO_INTL', status: 'ERROR', rows: 0, notes: e.message });
  }

  rows = dedupRawRecords_(rows);
  var fauxRecon = rows.map(function(r) {
    var sourcePrec = getSourcePrecedencePatched_(r.source_name);
    var freshness = computeFreshnessStatusPatched_(r.source_name, r.fetched_at);
    var ticket = computeTicketEstimateIfEligible_(r);
    return {
      film_id: '',
      canonical_title: r.film_title_raw,
      canonical_title_ar: r.film_title_ar_raw || '',
      release_year: r.release_year_hint || '',
      country: r.country,
      country_code: r.country_code,
      record_scope: r.record_scope,
      record_granularity: r.record_granularity,
      record_semantics: r.record_semantics,
      evidence_type: r.evidence_type,
      period_label_raw: r.period_label_raw,
      period_start_date: r.period_start_date,
      period_end_date: r.period_end_date,
      period_key: r.period_key,
      rank: r.rank,
      period_gross_local: r.period_gross_local,
      period_gross_usd: convertToUsd_(r.period_gross_local, r.currency),
      cumulative_gross_local: r.cumulative_gross_local,
      cumulative_gross_usd: convertToUsd_(r.cumulative_gross_local, r.currency),
      currency: r.currency,
      approx_ticket_estimate: ticket.value,
      ticket_estimate_confidence: ticket.confidence,
      source_name: r.source_name,
      source_precedence: sourcePrec,
      source_confidence: r.source_confidence,
      match_confidence: r.match_confidence,
      freshness_status: freshness,
      parser_confidence: r.parser_confidence,
      winning_raw_key: r.raw_key,
      alternate_raw_keys_json: '[]',
      source_url: r.source_url,
      notes: r.notes,
      reconciled_at: isoNow_(),
      film_title_raw: r.film_title_raw,
      film_title_ar_raw: r.film_title_ar_raw || ''
    };
  });

  return { rows: dedupReconLikeRows_(fauxRecon), status: status };
}

function discoverElCinemaWorkCandidates_(query, releaseYearHint) {
  var candidatesById = {};
  var searchQueries = [];
  var baseQueries = [query];
  var qTrim = String(query || '').trim();

  // Add a compact variant to catch punctuation/spacing differences.
  if (qTrim) baseQueries.push(qTrim.replace(/[^\u0600-\u06FFA-Za-z0-9]+/g, ' ').replace(/\s+/g, ' ').trim());
  unique_(baseQueries).forEach(function(q) {
    if (!q) return;
    searchQueries.push('site:elcinema.com/en/work/ "' + q + '"');
    searchQueries.push('site:elcinema.com/work/ "' + q + '"');
    if (releaseYearHint) {
      searchQueries.push('site:elcinema.com/en/work/ "' + q + '" "' + releaseYearHint + '"');
      searchQueries.push('site:elcinema.com/work/ "' + q + '" "' + releaseYearHint + '"');
    }
  });

  unique_(searchQueries).forEach(function(sq) {
    searchEngineCandidates_(sq).forEach(function(hit) {
      var workId = extractElCinemaWorkIdFromUrl_(hit.url);
      if (!workId) return;
      var score = scoreCandidateHit_(query, hit.title + ' ' + hit.snippet, releaseYearHint);
      if (!candidatesById[workId] || score > candidatesById[workId].score) {
        candidatesById[workId] = { work_id: workId, source_url: hit.url, matched_title: hit.title, score: score };
      }
    });
  });

  // Weak direct fallbacks from public elCinema pages still remain as a backup only.
  var directUrls = [
    'https://elcinema.com/en/boxoffice/',
    'https://elcinema.com/en/boxoffice/EG/',
    'https://elcinema.com/en/boxoffice/SA/'
  ];
  directUrls.forEach(function(url) {
    try {
      var html = fetchUrl_(url);
      var rx = /href="\/en\/work\/(\d+)\/"[^>]*>([\s\S]*?)<\/a>/gi;
      var m;
      while ((m = rx.exec(html)) !== null) {
        var workId = m[1];
        var title = htmlToText_(m[2]).trim();
        if (!title) continue;
        var score = scoreCandidateHit_(query, title, releaseYearHint);
        if (score < 0.75) continue;
        if (!candidatesById[workId] || score > candidatesById[workId].score) {
          candidatesById[workId] = { work_id: workId, source_url: url, matched_title: title, score: score };
        }
      }
    } catch (e) {}
  });

  var out = Object.keys(candidatesById).map(function(id) { return candidatesById[id]; });
  // Fetch metadata to validate the title against the actual work page before returning.
  out = out.map(function(c) {
    try {
      var meta = fetchElCinemaWorkMeta_(c.work_id);
      c.title_en = meta.title_en;
      c.title_ar = meta.title_ar;
      c.release_year = meta.release_year;
      c.score = Math.max(c.score, scoreResolvedTitle_(query, meta.title_en, meta.title_ar, meta.release_year, releaseYearHint));
    } catch (e) {}
    return c;
  }).filter(function(c) {
    return c.score >= 0.80;
  });

  out.sort(function(a, b) { return b.score - a.score; });
  return out.slice(0, 5);
}

function discoverBoxOfficeMojoCandidates_(query, releaseYearHint) {
  var byKey = {};
  var searchQueries = [];
  var variants = unique_([String(query || '').trim(), String(query || '').replace(/[^\u0600-\u06FFA-Za-z0-9]+/g, ' ').replace(/\s+/g, ' ').trim()]);

  variants.forEach(function(q) {
    if (!q) return;
    searchQueries.push('site:boxofficemojo.com/title/ "' + q + '"');
    searchQueries.push('site:boxofficemojo.com/release/ "' + q + '"');
    if (releaseYearHint) {
      searchQueries.push('site:boxofficemojo.com/title/ "' + q + '" "' + releaseYearHint + '"');
      searchQueries.push('site:boxofficemojo.com/release/ "' + q + '" "' + releaseYearHint + '"');
    }
  });

  unique_(searchQueries).forEach(function(sq) {
    searchEngineCandidates_(sq).forEach(function(hit) {
      var titleUrl = normalizeBoxOfficeMojoTitleUrl_(hit.url);
      var releaseUrl = normalizeBoxOfficeMojoReleaseUrl_(hit.url);
      if (!titleUrl && !releaseUrl) return;
      var key = releaseUrl || titleUrl;
      var score = scoreCandidateHit_(query, hit.title + ' ' + hit.snippet, releaseYearHint);
      if (!byKey[key] || score > byKey[key].score) {
        byKey[key] = { title_url: titleUrl, release_url: releaseUrl, source_url: hit.url, score: score, hit_title: hit.title };
      }
    });
  });

  var out = Object.keys(byKey).map(function(k) { return byKey[k]; });
  out.sort(function(a, b) { return b.score - a.score; });
  return out.slice(0, 8);
}

function searchEngineCandidates_(query) {
  var out = [];
  var seen = {};
  var normalizedQuery = String(query || '').trim();
  if (!normalizedQuery) return out;

  // Retry search fetches separately from normal page fetches because Bing/DDG can return
  // transient empty or throttled pages even when the request technically succeeds.
  var bingAttempts = fetchSearchWithRetry_('https://www.bing.com/search?format=rss&q=' + encodeURIComponent(normalizedQuery), {
    engine: 'bing_rss',
    attempts: 3,
    minLength: 40,
    validator: function(body) {
      return /<rss\b/i.test(body) && /<item\b/i.test(body);
    }
  });

  bingAttempts.forEach(function(xml) {
    var items = xml.match(/<item\b[\s\S]*?<\/item>/gi) || [];
    items.forEach(function(item) {
      var link = decodeHtmlEntitiesSimple_((item.match(/<link>([\s\S]*?)<\/link>/i) || [])[1] || '');
      var title = decodeHtmlEntitiesSimple_((item.match(/<title>([\s\S]*?)<\/title>/i) || [])[1] || '');
      var desc = decodeHtmlEntitiesSimple_((item.match(/<description>([\s\S]*?)<\/description>/i) || [])[1] || '');
      link = cleanupSearchResultUrl_(link);
      if (!link || seen[link]) return;
      seen[link] = true;
      out.push({ engine: 'bing_rss', url: link, title: title, snippet: desc });
    });
  });

  var ddgAttempts = fetchSearchWithRetry_('https://html.duckduckgo.com/html/?q=' + encodeURIComponent(normalizedQuery), {
    engine: 'duckduckgo_html',
    attempts: 3,
    minLength: 120,
    validator: function(body) {
      return /result__a/i.test(body) || /result-link/i.test(body);
    }
  });

  ddgAttempts.forEach(function(html) {
    var anchors = html.match(/<a[^>]+class="[^"]*(?:result__a|result-link)[^"]*"[^>]+href="[\s\S]*?<\/a>/gi) || [];
    anchors.forEach(function(a) {
      var href = (a.match(/href="([^"]+)"/i) || [])[1] || '';
      var title = htmlToText_(a);
      href = extractActualUrlFromSearchHref_(href);
      href = cleanupSearchResultUrl_(href);
      if (!href || seen[href]) return;
      seen[href] = true;
      out.push({ engine: 'duckduckgo_html', url: href, title: title, snippet: '' });
    });
  });

  return out;
}

function fetchSearchWithRetry_(url, options) {
  options = options || {};
  var attempts = Math.max(1, parseInt(options.attempts, 10) || 3);
  var validator = typeof options.validator === 'function' ? options.validator : function(body) { return !!String(body || '').trim(); };
  var minLength = Math.max(0, parseInt(options.minLength, 10) || 0);
  var responses = [];
  var sleepBase = 900;

  for (var i = 0; i < attempts; i++) {
    try {
      var body = fetchUrl_(url);
      var text = String(body || '');
      if (text.length >= minLength && validator(text)) {
        responses.push(text);
        break;
      }
    } catch (e) {}

    if (i < attempts - 1) {
      Utilities.sleep((i + 1) * sleepBase + Math.floor(Math.random() * 450));
    }
  }

  return responses;
}

function extractActualUrlFromSearchHref_(href) {
  var url = String(href || '').replace(/&amp;/g, '&');
  var m = url.match(/[?&]uddg=([^&]+)/i);
  if (m) {
    try { return decodeURIComponent(m[1]); } catch (e) { return m[1]; }
  }
  return url;
}

function cleanupSearchResultUrl_(url) {
  url = String(url || '').trim();
  if (!url) return '';
  if (url.indexOf('//') === 0) url = 'https:' + url;
  url = url.replace(/#.*$/, '');
  return url;
}

function fetchElCinemaWorkMeta_(workId) {
  var urlEn = 'https://elcinema.com/en/work/' + workId + '/';
  var htmlEn = fetchUrl_(urlEn);
  var textEn = htmlToText_(htmlEn);

  var titleEn = '';
  var year = '';

  var m1 = textEn.match(/#\s*(.*?)\s*\((20\d{2})\)/);
  if (m1) {
    titleEn = String(m1[1] || '').trim();
    year = m1[2] || '';
  }
  if (!titleEn) {
    var m2 = textEn.match(/Movie\s*-\s*(.*?)\s*-\s*(20\d{2})/i);
    if (m2) {
      titleEn = String(m2[1] || '').trim();
      year = year || m2[2] || '';
    }
  }

  var titleAr = '';
  try {
    var urlAr = 'https://elcinema.com/work/' + workId + '/';
    var htmlAr = fetchUrl_(urlAr);
    var textAr = htmlToText_(htmlAr);
    var a1 = textAr.match(/#\s*([\u0600-\u06FF\s]+?)\s*\((20\d{2})\)/);
    if (a1) {
      titleAr = normalizeWhitespace_(a1[1]).trim();
      year = year || a1[2] || '';
    } else {
      var a2 = textAr.match(/فيلم\s*-\s*([\u0600-\u06FF\s]+?)\s*-\s*(20\d{2})/);
      if (a2) {
        titleAr = normalizeWhitespace_(a2[1]).trim();
        year = year || a2[2] || '';
      }
    }
  } catch (e) {}

  return {
    work_id: workId,
    title_en: titleEn,
    title_ar: titleAr,
    release_year: year ? parseInt(year, 10) : '',
    source_url: urlEn
  };
}

function fetchElCinemaReleasedMarkets_(workId) {
  var url = 'https://elcinema.com/en/work/' + workId + '/released';
  var html = fetchUrl_(url);
  var text = htmlToText_(html);
  var rows = [];
  var rx = /(Egypt|Iraq|Saudi Arabia|Kuwait|Bahrain|Oman|United Arab Emirates|Jordan|Lebanon|Syria|Morocco)\s+(\d{1,2}\s+[A-Za-z]+\s+20\d{2})\s+(Yes|No)/gi;
  var m;
  while ((m = rx.exec(text)) !== null) {
    var code = findMarketCodeByCountry_(m[1]);
    if (!code) continue;
    rows.push({ country: m[1], country_code: code, release_date: toIsoDateFromLong_(m[2]), premiere: m[3] });
  }
  return rows;
}

function fetchBoxOfficeMojoTitleCandidate_(titleUrl) {
  var normalized = normalizeBoxOfficeMojoTitleUrl_(titleUrl);
  if (!normalized) throw new Error('Invalid Box Office Mojo title URL');
  var html = fetchUrl_(normalized);
  var text = htmlToText_(html);

  var title = '';
  var year = '';
  var m = text.match(/#\s*(.*?)\s*\((20\d{2})\)/);
  if (m) {
    title = String(m[1] || '').trim();
    year = m[2] || '';
  }

  var releaseUrls = [];
  var rx = /https:\/\/www\.boxofficemojo\.com\/release\/rl\d+\/weekend\/?/gi;
  var found = html.match(rx) || [];
  found.forEach(function(u) {
    u = normalizeBoxOfficeMojoReleaseUrl_(u);
    if (u && releaseUrls.indexOf(u) < 0) releaseUrls.push(u);
  });

  // Some pages use relative links.
  var relRx = /href="(\/release\/rl\d+\/weekend\/?)"/gi;
  var rm;
  while ((rm = relRx.exec(html)) !== null) {
    var full = normalizeBoxOfficeMojoReleaseUrl_('https://www.boxofficemojo.com' + rm[1]);
    if (full && releaseUrls.indexOf(full) < 0) releaseUrls.push(full);
  }

  var intlGross = '';
  var ig = text.match(/International\s+\(?100%\)?\s+\$([\d,]+)/i);
  if (ig) intlGross = parseMoney_(ig[1]);

  return {
    title_url: normalized,
    title_en: title,
    release_year: year ? parseInt(year, 10) : '',
    release_urls: releaseUrls,
    intl_gross_usd: intlGross,
    source_url: normalized
  };
}

function buildBoxOfficeMojoTitleSummaryRows_(titleInfo) {
  if (!titleInfo || !titleInfo.title_en || !titleInfo.intl_gross_usd) return [];
  return [buildRawRecord_({
    source_name: 'BOXOFFICEMOJO_TITLE',
    source_url: titleInfo.source_url,
    parser_confidence: 0.82,
    source_entity_id: titleInfo.title_url || titleInfo.source_url,
    country: 'All Territories',
    country_code: '',
    film_title_raw: titleInfo.title_en,
    film_title_ar_raw: '',
    release_year_hint: titleInfo.release_year || '',
    record_scope: 'global',
    record_granularity: 'lifetime',
    record_semantics: 'title_cumulative_total',
    source_confidence: 0.62,
    match_confidence: 0.88,
    evidence_type: 'title_performance',
    period_label_raw: 'lifetime',
    period_start_date: '',
    period_end_date: '',
    period_key: '',
    rank: '',
    period_gross_local: '',
    cumulative_gross_local: titleInfo.intl_gross_usd,
    currency: 'USD',
    admissions_actual: '',
    work_id: '',
    distributor: '',
    notes: 'International total from Box Office Mojo title page; global evidence only',
    raw_payload_json: JSON.stringify(titleInfo)
  })];
}

function fetchBoxOfficeMojoReleaseEvidence_(releaseUrl, query, yearHint) {
  var url = normalizeBoxOfficeMojoReleaseUrl_(releaseUrl);
  if (!url) throw new Error('Invalid Box Office Mojo release URL');
  var html = fetchUrl_(url);
  var text = htmlToText_(html);
  var rows = [];

  var title = '';
  var year = '';
  var titleMatch = text.match(/#\s*(.*?)\s*(?:\((20\d{2})\))?\s+Two cousins|#\s*(.*?)\s*(?:\((20\d{2})\))?\s+Title Summary/i);
  if (titleMatch) {
    title = (titleMatch[1] || titleMatch[3] || '').trim();
    year = titleMatch[2] || titleMatch[4] || '';
  }
  if (!title) {
    var simple = text.match(/#\s*(.*?)\s+Title Summary/i);
    if (simple) title = String(simple[1] || '').trim();
  }

  if (!titleMatchesQuery_(query, title, '', year, yearHint)) {
    // Keep going only if the URL itself still likely belongs to the query.
    var hitScore = scoreResolvedTitle_(query, title, '', year, yearHint);
    if (hitScore < 0.78) return [];
  }

  var territory = '';
  var gross = '';
  var opening = '';
  var theaters = '';
  var releaseDate = '';

  var terr1 = text.match(/All Territories\s+([A-Za-z ]+?)\s+Grosses/i);
  if (terr1) territory = normalizeWhitespace_(terr1[1]).trim();
  var terr2 = text.match(/Grosses\s+([A-Za-z ]+?)\s+\$([\d,]+)/i);
  if (terr2) {
    territory = territory || normalizeWhitespace_(terr2[1]).trim();
    gross = parseMoney_(terr2[2]);
  }
  var openM = text.match(/Opening\s+\$([\d,]+)/i);
  if (openM) opening = parseMoney_(openM[1]);
  var thM = text.match(/Opening\s+\$[\d,]+\s+(\d+)\s+theaters/i);
  if (thM) theaters = parseIntSafe_(thM[1]);
  var rdM = text.match(/Release Date\s+([A-Za-z]{3}\s+\d{1,2},\s+20\d{2})/i);
  if (rdM) releaseDate = toIsoDateFromLong_(rdM[1]);

  var code = findMarketCodeByCountry_(territory);
  if (code && gross) {
    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_RELEASE',
      source_url: url,
      parser_confidence: 0.88,
      source_entity_id: url + '|lifetime',
      country: CONFIG.MARKETS[code].country,
      country_code: code,
      film_title_raw: title,
      film_title_ar_raw: '',
      release_year_hint: year || yearHint || '',
      record_scope: 'title',
      record_granularity: 'lifetime',
      record_semantics: 'title_cumulative_total',
      source_confidence: 0.76,
      match_confidence: 0.90,
      evidence_type: 'title_performance',
      period_label_raw: 'lifetime',
      period_start_date: releaseDate || '',
      period_end_date: '',
      period_key: '',
      rank: '',
      period_gross_local: '',
      cumulative_gross_local: gross,
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor: '',
      notes: opening ? ('Opening=' + opening + ' USD; theaters=' + (theaters || '')) : 'Territory cumulative total from release page',
      raw_payload_json: JSON.stringify({ territory: territory, gross: gross, opening: opening, theaters: theaters, release_date: releaseDate })
    }));
  }

  // Weekend table rows: date label, rank, weekend gross, ... to-date
  var trMatches = normalizeWhitespace_(html).match(/<tr[\s\S]*?<\/tr>/gi) || [];
  trMatches.forEach(function(tr) {
    var tds = tr.match(/<td[\s\S]*?<\/td>/gi) || [];
    if (tds.length < 5 || !code) return;
    var cells = tds.map(function(td) { return htmlToText_(td).trim(); });
    var dateLabel = cells[0] || '';
    if (!/^[A-Z][a-z]{2}\s+\d{1,2}(?:-[A-Z][a-z]{2}\s+\d{1,2}|-\d{1,2})$/i.test(dateLabel)) return;

    var weekendGross = '';
    var toDate = '';
    // Weekend pages normally put weekend gross near cell 2 and to-date near the end.
    cells.forEach(function(c, idx) {
      if (!weekendGross && /^\$[\d,]+(?:\.\d+)?$/.test(c) && idx <= 3) weekendGross = parseMoney_(c);
    });
    for (var i = cells.length - 1; i >= 0; i--) {
      if (/^\$[\d,]+(?:\.\d+)?$/.test(cells[i])) {
        toDate = parseMoney_(cells[i]);
        break;
      }
    }

    var period = parseMojoReleaseWeekendLabelPatched_(dateLabel, releaseDate ? parseInt(String(releaseDate).slice(0, 4), 10) : (year || yearHint || new Date().getFullYear()));
    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_RELEASE',
      source_url: url,
      parser_confidence: weekendGross ? 0.82 : 0.70,
      source_entity_id: url + '|' + (period ? period.key : dateLabel),
      country: CONFIG.MARKETS[code].country,
      country_code: code,
      film_title_raw: title,
      film_title_ar_raw: '',
      release_year_hint: year || yearHint || '',
      record_scope: 'title',
      record_granularity: 'weekend',
      record_semantics: 'title_period_gross',
      source_confidence: 0.68,
      match_confidence: 0.88,
      evidence_type: 'title_performance',
      period_label_raw: dateLabel,
      period_start_date: period ? period.start : '',
      period_end_date: period ? period.end : '',
      period_key: period ? period.key : '',
      rank: cells[1] || '',
      period_gross_local: weekendGross || '',
      cumulative_gross_local: toDate || gross || '',
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor: '',
      notes: 'Weekend row parsed from Box Office Mojo territory release page',
      raw_payload_json: JSON.stringify({ cells: cells })
    }));
  });

  return dedupRawRecords_(rows);
}

function parseMojoReleaseWeekendLabelPatched_(label, fallbackYear) {
  label = String(label || '').trim();
  if (!label) return null;
  var year = parseInt(fallbackYear || new Date().getFullYear(), 10);
  var m1 = label.match(/^([A-Za-z]+)\s+(\d{1,2})-([A-Za-z]+)\s+(\d{1,2})$/);
  var m2 = label.match(/^([A-Za-z]+)\s+(\d{1,2})-(\d{1,2})$/);
  var sm, sd, em, ed;
  if (m1) {
    sm = MONTH_MAP[m1[1].slice(0, 3).toLowerCase()];
    sd = parseInt(m1[2], 10);
    em = MONTH_MAP[m1[3].slice(0, 3).toLowerCase()];
    ed = parseInt(m1[4], 10);
  } else if (m2) {
    sm = MONTH_MAP[m2[1].slice(0, 3).toLowerCase()];
    sd = parseInt(m2[2], 10);
    em = sm;
    ed = parseInt(m2[3], 10);
  } else {
    return null;
  }
  if (!sm || !em) return null;
  var start = Utilities.formatDate(new Date(year, sm - 1, sd), CONFIG.TIMEZONE, 'yyyy-MM-dd');
  var end = Utilities.formatDate(new Date(year, em - 1, ed), CONFIG.TIMEZONE, 'yyyy-MM-dd');
  return { start: start, end: end, key: start + '_' + end };
}

function extractElCinemaWorkIdFromUrl_(url) {
  var m = String(url || '').match(/elcinema\.com\/(?:en\/)?work\/(\d+)(?:\/|$)/i);
  return m ? m[1] : '';
}

function normalizeBoxOfficeMojoTitleUrl_(url) {
  var m = String(url || '').match(/https?:\/\/www\.boxofficemojo\.com\/title\/tt\d+\/?/i);
  if (!m) return '';
  return m[0].replace(/\/?(credits|releasegroup|cast|companycredits).*$/i, '/');
}

function normalizeBoxOfficeMojoReleaseUrl_(url) {
  var m = String(url || '').match(/https?:\/\/www\.boxofficemojo\.com\/release\/rl\d+\/weekend\/?/i);
  return m ? m[0].replace(/\/?$/, '/') : '';
}

function scoreCandidateHit_(query, text, releaseYearHint) {
  var s = titleSimilarity_(normalizeLatinTitle_(query), normalizeLatinTitle_(text));
  s = Math.max(s, titleSimilarity_(normalizeArabicTitle_(query), normalizeArabicTitle_(text)));
  if (releaseYearHint && String(text).indexOf(String(releaseYearHint)) >= 0) s += CONFIG.MATCHING.YEAR_BONUS;
  return Math.min(1, s);
}

function scoreResolvedTitle_(query, titleEn, titleAr, candidateYear, releaseYearHint) {
  var s = Math.max(
    titleSimilarity_(normalizeLatinTitle_(query), normalizeLatinTitle_(titleEn || '')),
    titleSimilarity_(normalizeArabicTitle_(query), normalizeArabicTitle_(titleAr || ''))
  );
  if (titleEn && normalizeLatinTitle_(query) === normalizeLatinTitle_(titleEn)) s = Math.max(s, 0.98);
  if (titleAr && normalizeArabicTitle_(query) && normalizeArabicTitle_(query) === normalizeArabicTitle_(titleAr)) s = Math.max(s, 0.99);
  if (releaseYearHint && candidateYear && String(releaseYearHint) === String(candidateYear)) s += CONFIG.MATCHING.YEAR_BONUS;
  if (releaseYearHint && candidateYear && String(releaseYearHint) !== String(candidateYear)) s -= CONFIG.MATCHING.YEAR_PENALTY;
  return Math.max(0, Math.min(1, s));
}

function extractYearHintFromQuery_(query) {
  var m = String(query || '').match(/\b(19|20)\d{2}\b/);
  return m ? parseInt(m[0], 10) : '';
}

function decodeHtmlEntitiesSimple_(text) {
  return String(text || '')
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#x2F;/gi, '/')
    .replace(/&#47;/g, '/');
}

function computeFreshnessStatusPatched_(sourceName, fetchedAt) {
  var limits = {
    ELCINEMA_TITLE_BOXOFFICE: 14,
    ELCINEMA_CHART: 7,
    BOXOFFICEMOJO_RELEASE: 14,
    BOXOFFICEMOJO_TITLE: 21,
    BOXOFFICEMOJO_INTL: 7,
    BOXOFFICEMOJO_AREA_YEAR: 30
  };
  var limit = limits[sourceName] || CONFIG.FRESHNESS_DAYS[sourceName] || 14;
  if (!fetchedAt) return 'unknown';
  var ageDays = (new Date().getTime() - new Date(fetchedAt).getTime()) / 86400000;
  return ageDays <= limit ? 'fresh' : 'stale';
}

function getSourcePrecedencePatched_(sourceName) {
  var local = {
    ELCINEMA_TITLE_BOXOFFICE: 100,
    ELCINEMA_CHART: 90,
    BOXOFFICEMOJO_RELEASE: 80,
    BOXOFFICEMOJO_TITLE: 60,
    BOXOFFICEMOJO_INTL: 40,
    BOXOFFICEMOJO_AREA_YEAR: 35
  };
  return local[sourceName] || CONFIG.SOURCE_PRECEDENCE[sourceName] || 0;
}

function computeTicketEstimateIfEligible_(rawRow) {
  if (!rawRow || rawRow.record_scope !== 'title') return { value: '', confidence: '' };
  if (!(rawRow.record_semantics === 'title_period_gross' || rawRow.record_semantics === 'title_cumulative_total')) return { value: '', confidence: '' };
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var priceRows = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.TICKET_PRICES));
  var p = priceRows.find(function(x) {
    return x.country_code === rawRow.country_code && String(x.currency || '').toUpperCase() === String(rawRow.currency || '').toUpperCase();
  });
  if (!p) return { value: '', confidence: '' };
  if (String(p.confidence || '').toLowerCase() === 'low') return { value: '', confidence: '' };
  var gross = rawRow.period_gross_local !== '' && rawRow.period_gross_local !== null && rawRow.period_gross_local !== undefined
    ? Number(rawRow.period_gross_local)
    : Number(rawRow.cumulative_gross_local);
  var ticketPrice = Number(p.avg_ticket_price_local || 0);
  if (!gross || !ticketPrice) return { value: '', confidence: '' };
  return { value: Math.round(gross / ticketPrice), confidence: String(p.confidence || '') };
}

function findMarketCodeByCountry_(countryLabel) {
  var tEn = normalizeLatinTitle_(countryLabel);
  var tAr = normalizeArabicTitle_(countryLabel);
  var exact = Object.keys(CONFIG.MARKETS).find(function(code) {
    return normalizeLatinTitle_(CONFIG.MARKETS[code].country) === tEn;
  });
  if (exact) return exact;

  var aliasesEn = {
    uae: 'AE',
    'united arab emirates': 'AE',
    emirati: 'AE',
    egypt: 'EG',
    'saudi arabia': 'SA',
    saudi: 'SA',
    kuwait: 'KW',
    bahrain: 'BH',
    lebanon: 'LB',
    qatar: 'QA',
    oman: 'OM',
    jordan: 'JO',
    iraq: '',
    syria: '',
    morocco: ''
  };
  if (aliasesEn[tEn] !== undefined) return aliasesEn[tEn] || '';

  var aliasesAr = {
    'الامارات العربية المتحدة': 'AE',
    'الإمارات العربية المتحدة': 'AE',
    'مصر': 'EG',
    'السعودية': 'SA',
    'المملكة العربية السعودية': 'SA',
    'الكويت': 'KW',
    'البحرين': 'BH',
    'لبنان': 'LB',
    'قطر': 'QA',
    'عمان': 'OM',
    'عُمان': 'OM',
    'الاردن': 'JO',
    'الأردن': 'JO',
    'العراق': '',
    'سوريا': '',
    'المغرب': ''
  };
  return aliasesAr[tAr] || '';
}


// ============================================================
// SECTION 14: V4.3 HARDENING OVERRIDES
// ============================================================

// Major goals of this patch:
// 1) promote high-confidence live title resolutions into Film_Master / Film_Aliases
// 2) improve Box Office Mojo extraction from title pages and search-result market surfaces
// 3) add analytical warnings and acquisition summary blocks
// 4) add market completeness scoring and source concentration warnings
// 5) keep lookup state stable and avoid misleading "no warnings" messaging

function renderLookup_(query, forceLive) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  var sheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  setupLookupSheet_(ss, query);
  clearLookupOutput_(sheet);
  sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).setValue(query);

  var recon = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RECON));
  var films = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.FILMS));
  var aliases = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.ALIASES));

  var match = matchQueryToFilm_(query, films, aliases);
  var relevant = [];
  var normQueryEn = normalizeLatinTitle_(query);
  var normQueryAr = normalizeArabicTitle_(query);

  if (match.film_id) {
    relevant = recon.filter(function(r) { return r.film_id === match.film_id; });
  } else {
    relevant = recon.filter(function(r) {
      var t1 = normalizeLatinTitle_(r.canonical_title || r.film_title_raw || '');
      var t2 = normalizeArabicTitle_(r.canonical_title_ar || r.film_title_ar_raw || '');
      return (t1 && (t1.indexOf(normQueryEn) >= 0 || normQueryEn.indexOf(t1) >= 0 || titleSimilarity_(normQueryEn, t1) >= 0.75)) ||
             (t2 && (t2.indexOf(normQueryAr) >= 0 || normQueryAr.indexOf(t2) >= 0 || titleSimilarity_(normQueryAr, t2) >= 0.80));
    });
  }

  var live = forceLive ? fetchLiveLookupSignals_(query, match) : { rows: [], status: [], releaseMarkets: [], promotedFilm: null, anomalies: [] };
  var promoted = live.promotedFilm || null;

  // Re-evaluate match if live promotion resolved a film.
  if (!match.film_id && promoted && promoted.film_id) {
    match = {
      film_id: promoted.film_id,
      title: promoted.canonical_title,
      release_year: promoted.release_year,
      score: promoted.identity_confidence || 0.96,
      identity_confidence: promoted.identity_confidence || 'live_promoted',
      canonical_title_ar: promoted.canonical_title_ar || ''
    };
  }

  var allRows = dedupReconLikeRows_(relevant.concat(live.rows || []));
  if (match.film_id) {
    allRows = allRows.map(function(r) {
      if (!r.film_id) r.film_id = match.film_id;
      if (!r.canonical_title) r.canonical_title = match.title || r.film_title_raw || '';
      if (!r.canonical_title_ar) r.canonical_title_ar = match.canonical_title_ar || r.film_title_ar_raw || '';
      if (!r.release_year) r.release_year = match.release_year || r.release_year_hint || '';
      return r;
    });
  }

  var releaseMarkets = unique_((live.releaseMarkets || []).map(function(x) { return x.country_code; }).filter(Boolean));
  var warnings = buildAcquisitionWarnings_(query, allRows, live.status || [], releaseMarkets, match, live.anomalies || []);
  var summary = buildAcquisitionSummary_(allRows, releaseMarkets, match, live.status || []);
  var coverage = buildMarketCoverageV2_(allRows, releaseMarkets);
  var perfRows = buildComparablePerformanceRowsV2_(allRows);
  var chartRows = buildChartRowsV2_(allRows);
  var sourceMatrix = buildSourceCoverageMatrix_(allRows, live.status || []);

  var startRow = 6;

  var overview = [
    ['Matched Film', summary.matched_film || match.title || query, 'Match Confidence', summary.match_confidence || (match.score || '')],
    ['Film ID', match.film_id || '', 'Identity Status', summary.identity_status || (match.film_id ? 'matched' : 'unmatched')],
    ['Release Year', summary.release_year || match.release_year || '', 'Live Refresh', forceLive ? isoNow_() : 'No']
  ];
  sheet.getRange(startRow, 1, overview.length, 4).setValues(overview);
  styleBlockHeader_(sheet.getRange(startRow, 1, 1, 4), '#E5E7EB');

  var summaryRow = startRow + 4;
  sheet.getRange(summaryRow, 1).setValue('Acquisition Summary').setFontWeight('bold').setBackground('#C7D2FE');
  var summaryBody = [
    ['Evidence Strength', summary.evidence_strength],
    ['Best-Supported Markets', summary.best_markets || 'None'],
    ['Missing Priority Markets', summary.missing_priority_markets || 'None'],
    ['Weekly Evidence Markets', summary.weekly_markets || 'None'],
    ['Source Concentration', summary.source_concentration || 'Unknown'],
    ['Analyst Take', summary.analyst_take || 'Insufficient evidence for a confident acquisition read.']
  ];
  sheet.getRange(summaryRow + 1, 1, summaryBody.length, 2).setValues(summaryBody);
  styleLabelValueBlock_(sheet, summaryRow + 1, summaryBody.length, 2);

  var warnRow = summaryRow + summaryBody.length + 2;
  sheet.getRange(warnRow, 1).setValue('Acquisition Warnings').setFontWeight('bold').setBackground('#FDE68A');
  var warnValues = warnings.length ? warnings.map(function(w) { return [w]; }) : [['No critical warnings generated.']];
  sheet.getRange(warnRow + 1, 1, warnValues.length, 1).setValues(warnValues);
  if (warnings.length) sheet.getRange(warnRow + 1, 1, warnValues.length, 1).setFontColor('#7C2D12');

  var coverageRow = warnRow + warnValues.length + 3;
  sheet.getRange(coverageRow, 1).setValue('Market Coverage Summary').setFontWeight('bold').setBackground('#DBEAFE');
  var coverageHeader = [['Country', 'Coverage Score', 'Strongest Evidence', 'Latest Period', 'Latest Gross', 'Latest Total', 'Weeks/Records', 'Freshness', 'Primary Source']];
  sheet.getRange(coverageRow + 1, 1, 1, coverageHeader[0].length).setValues(coverageHeader).setFontWeight('bold');
  if (coverage.length) sheet.getRange(coverageRow + 2, 1, coverage.length, coverageHeader[0].length).setValues(coverage);

  var perfRow = coverageRow + 3 + Math.max(coverage.length, 1);
  sheet.getRange(perfRow, 1).setValue('Comparable Title Performance Evidence').setFontWeight('bold').setBackground('#D1FAE5');
  var perfHeader = [['Country', 'Granularity', 'Period', 'Period Gross (Local)', 'Currency', 'Cumulative (Local)', 'Approx Tickets', 'Ticket Confidence', 'Source', 'Freshness', 'Evidence Grade']];
  sheet.getRange(perfRow + 1, 1, 1, perfHeader[0].length).setValues(perfHeader).setFontWeight('bold');
  if (perfRows.length) sheet.getRange(perfRow + 2, 1, perfRows.length, perfHeader[0].length).setValues(perfRows);
  else sheet.getRange(perfRow + 2, 1).setValue('No title-level comparable evidence available.');

  var chartRow = perfRow + 4 + Math.max(perfRows.length, 1);
  sheet.getRange(chartRow, 1).setValue('Chart Mentions / Market-Level Signals (Do Not Treat as Title Performance)').setFontWeight('bold').setBackground('#FECACA');
  var chartHeader = [['Country', 'Period', 'Top Title Mentioned', 'Metric', 'Currency', 'Source', 'Freshness']];
  sheet.getRange(chartRow + 1, 1, 1, chartHeader[0].length).setValues(chartHeader).setFontWeight('bold');
  if (chartRows.length) sheet.getRange(chartRow + 2, 1, chartRows.length, chartHeader[0].length).setValues(chartRows);
  else sheet.getRange(chartRow + 2, 1).setValue('No chart-mention rows found.');

  var sourceRow = chartRow + 4 + Math.max(chartRows.length, 1);
  sheet.getRange(sourceRow, 1).setValue('Source Coverage Matrix').setFontWeight('bold').setBackground('#F3E8FF');
  var sourceHeader = [['Source', 'Type', 'Markets', 'Rows', 'Status / Notes']];
  sheet.getRange(sourceRow + 1, 1, 1, sourceHeader[0].length).setValues(sourceHeader).setFontWeight('bold');
  if (sourceMatrix.length) sheet.getRange(sourceRow + 2, 1, sourceMatrix.length, sourceHeader[0].length).setValues(sourceMatrix);

  var statusRow = sourceRow + 4 + Math.max(sourceMatrix.length, 1);
  sheet.getRange(statusRow, 1).setValue('Live Source Status').setFontWeight('bold').setBackground('#E9D5FF');
  var statusHeader = [['Source', 'Status', 'Rows', 'Notes']];
  sheet.getRange(statusRow + 1, 1, 1, 4).setValues(statusHeader).setFontWeight('bold');
  var statuses = (live.status && live.status.length) ? live.status : [{ source: 'local_only', status: 'Not refreshed live', rows: '', notes: 'Used local reconciled data only' }];
  var statusValues = statuses.map(function(s) { return [s.source, s.status, s.rows, s.notes]; });
  sheet.getRange(statusRow + 2, 1, statusValues.length, 4).setValues(statusValues);

  autoResize_(sheet, 14);
}

function fetchLiveLookupSignals_(query, match) {
  var status = [];
  var rows = [];
  var yearHint = extractYearHintFromQuery_(query) || (match && match.release_year) || '';
  var discoveredWorkIds = {};
  var discoveredMojoReleaseUrls = {};
  var discoveredMojoTitleUrls = {};
  var releaseMarkets = [];
  var anomalies = [];

  // 1) Resolve elCinema title pages first.
  try {
    var eCandidates = discoverElCinemaWorkCandidates_(query, yearHint);
    status.push({ source: 'ELCINEMA_DISCOVERY', status: 'OK', rows: eCandidates.length, notes: eCandidates.length ? 'Resolved elCinema work candidates from title discovery' : 'No elCinema title candidates resolved' });
    eCandidates.forEach(function(c) { if (c.work_id) discoveredWorkIds[c.work_id] = c; });
  } catch (e) {
    status.push({ source: 'ELCINEMA_DISCOVERY', status: 'ERROR', rows: 0, notes: e.message });
    anomalies.push('elCinema discovery failed: ' + e.message);
  }

  Object.keys(discoveredWorkIds).slice(0, 5).forEach(function(workId) {
    try {
      var detail = fetchElCinemaTitleBoxOffice_(workId).filter(function(r) {
        return titleMatchesQuery_(query, r.film_title_raw, r.film_title_ar_raw || '', r.release_year_hint, yearHint);
      }).map(function(r) {
        r.match_confidence = Math.max(Number(r.match_confidence || 0), 0.92);
        return r;
      });
      rows = rows.concat(detail);
      status.push({ source: 'ELCINEMA_TITLE_BOXOFFICE:' + workId, status: 'OK', rows: detail.length, notes: detail.length ? 'Fetched title-specific weekly history' : 'No weekly history rows on title box office page' });
    } catch (e) {
      status.push({ source: 'ELCINEMA_TITLE_BOXOFFICE:' + workId, status: 'ERROR', rows: 0, notes: e.message });
      anomalies.push('elCinema title box office failed for ' + workId + ': ' + e.message);
    }

    try {
      var released = fetchElCinemaReleasedMarkets_(workId);
      releaseMarkets = releaseMarkets.concat(released);
      status.push({ source: 'ELCINEMA_RELEASED:' + workId, status: 'OK', rows: released.length, notes: released.length ? ('Release dates found for: ' + released.map(function(x){ return x.country_code; }).join(', ')) : 'No release-date markets found' });
    } catch (e) {
      status.push({ source: 'ELCINEMA_RELEASED:' + workId, status: 'ERROR', rows: 0, notes: e.message });
      anomalies.push('elCinema release dates failed for ' + workId + ': ' + e.message);
    }
  });

  // Supplemental current chart.
  try {
    var chart = fetchElCinemaCurrentChart_().filter(function(r) {
      return titleMatchesQuery_(query, r.film_title_raw, r.film_title_ar_raw || '', r.release_year_hint, yearHint);
    }).map(function(r) { r.match_confidence = Math.max(Number(r.match_confidence || 0), 0.80); return r; });
    rows = rows.concat(chart);
    status.push({ source: 'ELCINEMA_CHART', status: 'OK', rows: chart.length, notes: 'Current Egypt chart title hits (supplemental only)' });
  } catch (e) {
    status.push({ source: 'ELCINEMA_CHART', status: 'ERROR', rows: 0, notes: e.message });
  }

  // 2) Resolve Box Office Mojo title and release pages via search engines.
  try {
    var mCandidates = discoverBoxOfficeMojoCandidates_(query, yearHint);
    status.push({ source: 'BOXOFFICEMOJO_DISCOVERY', status: 'OK', rows: mCandidates.length, notes: mCandidates.length ? 'Resolved Box Office Mojo title/release candidates' : 'No Box Office Mojo title/release candidates resolved' });
    mCandidates.forEach(function(c) {
      if (c.release_url) discoveredMojoReleaseUrls[c.release_url] = c;
      if (c.title_url) discoveredMojoTitleUrls[c.title_url] = c;
    });
  } catch (e) {
    status.push({ source: 'BOXOFFICEMOJO_DISCOVERY', status: 'ERROR', rows: 0, notes: e.message });
    anomalies.push('Box Office Mojo discovery failed: ' + e.message);
  }

  // 3) Title pages now emit area rows directly whenever possible.
  Object.keys(discoveredMojoTitleUrls).slice(0, 4).forEach(function(titleUrl) {
    try {
      var titleInfo = fetchBoxOfficeMojoTitleCandidate_(titleUrl);
      var titleRows = buildBoxOfficeMojoTitleSummaryRows_(titleInfo).filter(function(r) {
        return !r.country_code || r.country_code === '' || titleMatchesQuery_(query, r.film_title_raw, '', r.release_year_hint, yearHint);
      });
      rows = rows.concat(titleRows);
      (titleInfo.release_urls || []).forEach(function(releaseUrl) { discoveredMojoReleaseUrls[releaseUrl] = discoveredMojoReleaseUrls[releaseUrl] || { release_url: releaseUrl }; });
      status.push({ source: 'BOXOFFICEMOJO_TITLE', status: 'OK', rows: titleRows.length, notes: titleRows.length ? 'Title page yielded direct area/total evidence' : 'Title page resolved but yielded no usable rows' });
      if (!titleRows.length) anomalies.push('Box Office Mojo title page resolved with zero usable rows: ' + titleUrl);
    } catch (e) {
      status.push({ source: 'BOXOFFICEMOJO_TITLE', status: 'ERROR', rows: 0, notes: e.message });
      anomalies.push('Box Office Mojo title page failed for ' + titleUrl + ': ' + e.message);
    }
  });

  // 4) Territory release pages.
  Object.keys(discoveredMojoReleaseUrls).slice(0, 8).forEach(function(url) {
    try {
      var relRows = fetchBoxOfficeMojoReleaseEvidence_(url, query, yearHint);
      rows = rows.concat(relRows);
      status.push({ source: 'BOXOFFICEMOJO_RELEASE', status: 'OK', rows: relRows.length, notes: relRows.length ? 'Fetched territory-specific release evidence' : 'Release page resolved but yielded no usable rows' });
    } catch (e) {
      status.push({ source: 'BOXOFFICEMOJO_RELEASE', status: 'ERROR', rows: 0, notes: e.message });
      anomalies.push('Box Office Mojo release page failed for ' + url + ': ' + e.message);
    }
  });

  // 5) Search-result market surfaces for Box Office Mojo, treated only as market/chart signals.
  try {
    var marketSignals = discoverBoxOfficeMojoMarketSignals_(query, yearHint, releaseMarkets);
    rows = rows.concat(marketSignals.rows || []);
    (marketSignals.status || []).forEach(function(s) { status.push(s); });
    anomalies = anomalies.concat(marketSignals.anomalies || []);
  } catch (e) {
    status.push({ source: 'BOXOFFICEMOJO_SEARCH_MARKETS', status: 'ERROR', rows: 0, notes: e.message });
  }

  rows = dedupRawRecords_(rows);
  var fauxRecon = rows.map(function(r) {
    var t = computeTicketEstimateIfEligible_(r);
    return {
      film_id: '',
      canonical_title: r.film_title_raw,
      canonical_title_ar: r.film_title_ar_raw || '',
      release_year: r.release_year_hint || '',
      country: r.country,
      country_code: r.country_code,
      record_scope: r.record_scope,
      record_granularity: r.record_granularity,
      record_semantics: r.record_semantics,
      evidence_type: r.evidence_type,
      period_label_raw: r.period_label_raw,
      period_start_date: r.period_start_date,
      period_end_date: r.period_end_date,
      period_key: r.period_key,
      rank: r.rank,
      period_gross_local: r.period_gross_local,
      period_gross_usd: convertToUsd_(r.period_gross_local, r.currency),
      cumulative_gross_local: r.cumulative_gross_local,
      cumulative_gross_usd: convertToUsd_(r.cumulative_gross_local, r.currency),
      currency: r.currency,
      approx_ticket_estimate: t.value,
      ticket_estimate_confidence: t.confidence,
      source_name: r.source_name,
      source_precedence: getSourcePrecedencePatched_(r.source_name),
      source_confidence: r.source_confidence,
      match_confidence: r.match_confidence,
      freshness_status: computeFreshnessStatusPatched_(r.source_name, r.fetched_at),
      parser_confidence: r.parser_confidence,
      winning_raw_key: r.raw_key,
      alternate_raw_keys_json: '[]',
      source_url: r.source_url,
      notes: r.notes,
      reconciled_at: isoNow_(),
      film_title_raw: r.film_title_raw,
      film_title_ar_raw: r.film_title_ar_raw || ''
    };
  });

  fauxRecon = dedupReconLikeRows_(fauxRecon);

  var promotedFilm = null;
  try {
    promotedFilm = promoteLiveCanonicalFilm_(SpreadsheetApp.getActiveSpreadsheet(), query, fauxRecon, releaseMarkets);
    if (promotedFilm && promotedFilm.film_id) {
      fauxRecon = fauxRecon.map(function(r) {
        r.film_id = promotedFilm.film_id;
        r.canonical_title = promotedFilm.canonical_title || r.canonical_title || r.film_title_raw || '';
        r.canonical_title_ar = promotedFilm.canonical_title_ar || r.canonical_title_ar || r.film_title_ar_raw || '';
        r.release_year = promotedFilm.release_year || r.release_year || '';
        return r;
      });
    }
  } catch (e) {
    anomalies.push('Canonical promotion failed: ' + e.message);
  }

  try {
    appendReviewItemsForLookupAnomalies_(SpreadsheetApp.getActiveSpreadsheet(), query, fauxRecon, status, anomalies);
  } catch (e) {}

  return { rows: fauxRecon, status: status, releaseMarkets: releaseMarkets, promotedFilm: promotedFilm, anomalies: anomalies };
}

function fetchBoxOfficeMojoTitleCandidate_(titleUrl) {
  var normalized = normalizeBoxOfficeMojoTitleUrl_(titleUrl);
  if (!normalized) throw new Error('Invalid Box Office Mojo title URL');
  var html = fetchUrl_(normalized);
  var text = htmlToText_(html);

  var title = '';
  var year = '';
  var m = text.match(/#\s*(.*?)\s*\((20\d{2})\)/);
  if (m) {
    title = String(m[1] || '').trim();
    year = m[2] || '';
  }
  if (!title) {
    var og = html.match(/<meta[^>]+property="og:title"[^>]+content="([^"]+)"/i);
    if (og) {
      var ogt = decodeHtmlEntitiesSimple_(og[1] || '');
      var mOg = ogt.match(/^(.*?)\s*\((20\d{2})\)/);
      if (mOg) {
        title = normalizeWhitespace_(mOg[1]).trim();
        year = mOg[2] || year;
      }
    }
  }

  var releaseUrls = [];
  var rx = /(?:https:\/\/www\.boxofficemojo\.com)?(\/release\/rl\d+\/weekend\/?)/gi;
  var found;
  while ((found = rx.exec(html)) !== null) {
    var full = normalizeBoxOfficeMojoReleaseUrl_('https://www.boxofficemojo.com' + found[1]);
    if (full && releaseUrls.indexOf(full) < 0) releaseUrls.push(full);
  }

  var intlGross = '';
  var ig = text.match(/International\s+\(?100%\)?\s+\$([\d,]+)/i);
  if (ig) intlGross = parseMoney_(ig[1]);

  var areaRows = parseBoxOfficeMojoTitleAreaRows_(html, text, title, year);
  return {
    title_url: normalized,
    title_en: title,
    release_year: year ? parseInt(year, 10) : '',
    release_urls: releaseUrls,
    intl_gross_usd: intlGross,
    area_rows: areaRows,
    source_url: normalized
  };
}

function parseBoxOfficeMojoTitleAreaRows_(html, text, title, year) {
  var rows = [];
  var seen = {};
  Object.keys(CONFIG.MARKETS).forEach(function(code) {
    var country = CONFIG.MARKETS[code].country;
    var esc = escapeRegex_(country).replace(/\s+/g, '\\s*');
    var rx = new RegExp(esc + '\\s*([A-Z][a-z]{2}\\s+\\d{1,2},\\s+20\\d{2})\\s*\\$([\\d,]+)(?:\\s*\\$([\\d,]+))?', 'ig');
    var m;
    while ((m = rx.exec(text)) !== null) {
      var releaseDate = toIsoDateFromLong_(m[1]);
      var opening = parseMoney_(m[2]);
      var gross = parseMoney_(m[3] || m[2]);
      var key = code + '|' + gross + '|' + releaseDate;
      if (seen[key]) continue;
      seen[key] = true;
      rows.push(buildRawRecord_({
        source_name: 'BOXOFFICEMOJO_TITLE',
        source_url: '',
        parser_confidence: 0.86,
        source_entity_id: 'title_area|' + key,
        country: country,
        country_code: code,
        film_title_raw: title || '',
        film_title_ar_raw: '',
        release_year_hint: year || '',
        record_scope: 'title',
        record_granularity: 'lifetime',
        record_semantics: 'title_cumulative_total',
        source_confidence: 0.72,
        match_confidence: 0.90,
        evidence_type: 'title_performance',
        period_label_raw: 'lifetime',
        period_start_date: releaseDate || '',
        period_end_date: '',
        period_key: '',
        rank: '',
        period_gross_local: '',
        cumulative_gross_local: gross,
        currency: 'USD',
        admissions_actual: '',
        work_id: '',
        distributor: '',
        notes: opening && gross ? ('Title page area row; opening=' + opening + ' USD') : 'Title page area row',
        raw_payload_json: JSON.stringify({ country: country, release_date: releaseDate, opening: opening, gross: gross })
      }));
    }
  });
  return rows;
}

function buildBoxOfficeMojoTitleSummaryRows_(titleInfo) {
  if (!titleInfo) return [];
  var rows = [];
  if (titleInfo.area_rows && titleInfo.area_rows.length) rows = rows.concat(titleInfo.area_rows);
  if (titleInfo.title_en && titleInfo.intl_gross_usd) {
    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_TITLE',
      source_url: titleInfo.source_url,
      parser_confidence: 0.80,
      source_entity_id: titleInfo.title_url || titleInfo.source_url,
      country: 'All Territories',
      country_code: '',
      film_title_raw: titleInfo.title_en,
      film_title_ar_raw: '',
      release_year_hint: titleInfo.release_year || '',
      record_scope: 'global',
      record_granularity: 'lifetime',
      record_semantics: 'title_cumulative_total',
      source_confidence: 0.58,
      match_confidence: 0.88,
      evidence_type: 'title_performance',
      period_label_raw: 'lifetime',
      period_start_date: '',
      period_end_date: '',
      period_key: '',
      rank: '',
      period_gross_local: '',
      cumulative_gross_local: titleInfo.intl_gross_usd,
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor: '',
      notes: 'International total from Box Office Mojo title page; global evidence only',
      raw_payload_json: JSON.stringify(titleInfo)
    }));
  }
  return dedupRawRecords_(rows);
}

function fetchBoxOfficeMojoReleaseEvidence_(releaseUrl, query, yearHint) {
  var url = normalizeBoxOfficeMojoReleaseUrl_(releaseUrl);
  if (!url) throw new Error('Invalid Box Office Mojo release URL');
  var html = fetchUrl_(url);
  var text = htmlToText_(html);
  var rows = [];

  var title = '';
  var year = '';
  var titleMatch = text.match(/#\s*(.*?)\s*\((20\d{2})\)\s+Title Summary/i) || text.match(/#\s*(.*?)\s+Title Summary/i);
  if (titleMatch) {
    title = String(titleMatch[1] || '').trim();
    year = titleMatch[2] || '';
  }
  if (!title) {
    var og = html.match(/<meta[^>]+property="og:title"[^>]+content="([^"]+)"/i);
    if (og) {
      var txt = decodeHtmlEntitiesSimple_(og[1]);
      var mog = txt.match(/^(.*?)\s*\((20\d{2})\)/);
      if (mog) {
        title = normalizeWhitespace_(mog[1]).trim();
        year = mog[2] || year;
      }
    }
  }

  if (!titleMatchesQuery_(query, title, '', year, yearHint)) {
    var hitScore = scoreResolvedTitle_(query, title, '', year, yearHint);
    if (hitScore < 0.72) return [];
  }

  var countryCode = '';
  var country = '';
  Object.keys(CONFIG.MARKETS).forEach(function(code) {
    if (countryCode) return;
    var cname = CONFIG.MARKETS[code].country;
    if (new RegExp(escapeRegex_(cname) + '\\s+Grosses', 'i').test(text) || new RegExp('Grosses\\s+' + escapeRegex_(cname), 'i').test(text)) {
      countryCode = code;
      country = cname;
    }
  });

  var releaseDate = '';
  var rdM = text.match(/Release Date\s+([A-Za-z]{3}\s+\d{1,2},\s+20\d{2})/i);
  if (rdM) releaseDate = toIsoDateFromLong_(rdM[1]);
  var opening = '';
  var openM = text.match(/Opening\s+\$([\d,]+)/i);
  if (openM) opening = parseMoney_(openM[1]);
  var theaters = '';
  var thM = text.match(/Opening\s+\$[\d,]+\s+(\d+)\s+theaters/i);
  if (thM) theaters = parseIntSafe_(thM[1]);

  var gross = '';
  if (country) {
    var rxGross = new RegExp(escapeRegex_(country) + '\\s+Grosses\\s*\\$([\\d,]+)', 'i');
    var g1 = text.match(rxGross);
    if (g1) gross = parseMoney_(g1[1]);
    if (!gross) {
      var rxGross2 = new RegExp('Grosses\\s+' + escapeRegex_(country) + '\\s+\\$([\\d,]+)', 'i');
      var g2 = text.match(rxGross2);
      if (g2) gross = parseMoney_(g2[1]);
    }
  }

  if (countryCode && gross) {
    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_RELEASE',
      source_url: url,
      parser_confidence: 0.88,
      source_entity_id: url + '|lifetime',
      country: country,
      country_code: countryCode,
      film_title_raw: title,
      film_title_ar_raw: '',
      release_year_hint: year || yearHint || '',
      record_scope: 'title',
      record_granularity: 'lifetime',
      record_semantics: 'title_cumulative_total',
      source_confidence: 0.78,
      match_confidence: 0.90,
      evidence_type: 'title_performance',
      period_label_raw: 'lifetime',
      period_start_date: releaseDate || '',
      period_end_date: '',
      period_key: '',
      rank: '',
      period_gross_local: '',
      cumulative_gross_local: gross,
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor: '',
      notes: opening ? ('Opening=' + opening + ' USD; theaters=' + (theaters || '')) : 'Territory cumulative total from release page',
      raw_payload_json: JSON.stringify({ territory: country, gross: gross, opening: opening, theaters: theaters, release_date: releaseDate })
    }));
  }

  var trMatches = normalizeWhitespace_(html).match(/<tr[\s\S]*?<\/tr>/gi) || [];
  trMatches.forEach(function(tr) {
    var tds = tr.match(/<td[\s\S]*?<\/td>/gi) || [];
    if (tds.length < 4 || !countryCode) return;
    var cells = tds.map(function(td) { return htmlToText_(td).trim(); });
    var dateLabel = cells[0] || '';
    if (!/^([A-Z][a-z]{2}\s+\d{1,2}(?:-[A-Z][a-z]{2}\s+\d{1,2}|-\d{1,2}))$/i.test(dateLabel)) return;

    var weekendGross = '';
    var toDate = '';
    cells.forEach(function(c, idx) {
      if (!weekendGross && /^\$[\d,]+(?:\.\d+)?$/.test(c) && idx <= 4) weekendGross = parseMoney_(c);
    });
    for (var i = cells.length - 1; i >= 0; i--) {
      if (/^\$[\d,]+(?:\.\d+)?$/.test(cells[i])) {
        toDate = parseMoney_(cells[i]);
        break;
      }
    }

    var period = parseMojoReleaseWeekendLabelPatched_(dateLabel, releaseDate ? parseInt(String(releaseDate).slice(0, 4), 10) : (year || yearHint || new Date().getFullYear()));
    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_RELEASE',
      source_url: url,
      parser_confidence: weekendGross ? 0.82 : 0.68,
      source_entity_id: url + '|' + (period ? period.key : dateLabel),
      country: country,
      country_code: countryCode,
      film_title_raw: title,
      film_title_ar_raw: '',
      release_year_hint: year || yearHint || '',
      record_scope: 'title',
      record_granularity: 'weekend',
      record_semantics: 'title_period_gross',
      source_confidence: 0.70,
      match_confidence: 0.88,
      evidence_type: 'title_performance',
      period_label_raw: dateLabel,
      period_start_date: period ? period.start : '',
      period_end_date: period ? period.end : '',
      period_key: period ? period.key : '',
      rank: cells[1] || '',
      period_gross_local: weekendGross || '',
      cumulative_gross_local: toDate || gross || '',
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor: '',
      notes: 'Weekend row parsed from Box Office Mojo territory release page',
      raw_payload_json: JSON.stringify({ cells: cells })
    }));
  });

  return dedupRawRecords_(rows);
}

function discoverBoxOfficeMojoMarketSignals_(query, yearHint, releaseMarkets) {
  var rows = [];
  var status = [];
  var anomalies = [];
  var year = yearHint || new Date().getFullYear();
  var targetCodes = unique_((releaseMarkets || []).map(function(x) { return x.country_code; }).filter(Boolean));
  if (!targetCodes.length) targetCodes = ['AE', 'SA', 'EG'];

  targetCodes.slice(0, 6).forEach(function(code) {
    try {
      var cname = CONFIG.MARKETS[code].country;
      var queries = [
        'site:boxofficemojo.com/year/ "' + query + '" "' + cname + '" "' + year + '"',
        'site:boxofficemojo.com/chart/top_release_gross_nth_weekend/ "' + query + '" "' + cname + '"',
        'site:boxofficemojo.com/chart/never_in_top/ "' + query + '" "' + cname + '"'
      ];
      var localRows = [];
      unique_(queries).forEach(function(sq) {
        searchEngineCandidates_(sq).forEach(function(hit) {
          var parsed = parseBoxOfficeMojoSearchSnippetSignal_(hit, query, code, year);
          if (parsed) localRows.push(parsed);
        });
      });
      localRows = dedupRawRecords_(localRows);
      rows = rows.concat(localRows);
      status.push({ source: 'BOXOFFICEMOJO_SEARCH_MARKETS:' + code, status: 'OK', rows: localRows.length, notes: localRows.length ? 'Search-result market signals found' : 'No search-result market signals found' });
    } catch (e) {
      status.push({ source: 'BOXOFFICEMOJO_SEARCH_MARKETS:' + code, status: 'ERROR', rows: 0, notes: e.message });
      anomalies.push('Market search failed for ' + code + ': ' + e.message);
    }
  });
  return { rows: rows, status: status, anomalies: anomalies };
}

function parseBoxOfficeMojoSearchSnippetSignal_(hit, query, countryCode, yearHint) {
  var text = normalizeWhitespace_((hit.title || '') + ' ' + (hit.snippet || ''));
  if (!text) return null;
  var lower = normalizeLatinTitle_(text);
  var q = normalizeLatinTitle_(query);
  if (q && lower.indexOf(q) < 0 && titleSimilarity_(q, lower) < 0.65) return null;
  var country = CONFIG.MARKETS[countryCode].country;
  if (normalizeLatinTitle_(text).indexOf(normalizeLatinTitle_(country)) < 0) return null;
  var dollars = text.match(/\$([\d,]+)/g) || [];
  var gross = dollars.length ? parseMoney_(String(dollars[dollars.length - 1]).replace('$', '')) : '';
  var weekend = dollars.length > 1 ? parseMoney_(String(dollars[0]).replace('$', '')) : '';
  var weekCount = '';
  var wc = text.match(/\b(\d+)\b/);
  if (wc) weekCount = parseIntSafe_(wc[1]);
  var url = cleanupSearchResultUrl_(hit.url || '');
  var semantics = /top_release_gross_nth_weekend|never_in_top/i.test(url) ? 'market_chart_topline' : 'title_cumulative_total';
  var scope = semantics === 'market_chart_topline' ? 'market' : 'title';
  var granularity = /top_release_gross_nth_weekend/i.test(url) ? 'weekend' : 'lifetime';
  var note = /never_in_top/i.test(url) ? 'Search-result signal from never_in_top chart; not direct title history' : (/top_release_gross_nth_weekend/i.test(url) ? 'Search-result signal from nth-weekend chart' : 'Search-result signal from year chart');
  return buildRawRecord_({
    source_name: 'BOXOFFICEMOJO_SEARCH_MARKET',
    source_url: url,
    parser_confidence: 0.66,
    source_entity_id: url + '|' + countryCode + '|' + normalizeLatinTitle_(query),
    country: country,
    country_code: countryCode,
    film_title_raw: query,
    film_title_ar_raw: '',
    release_year_hint: yearHint || '',
    record_scope: scope,
    record_granularity: granularity,
    record_semantics: semantics,
    source_confidence: 0.42,
    match_confidence: 0.76,
    evidence_type: semantics === 'market_chart_topline' ? 'chart_signal' : 'title_performance',
    period_label_raw: yearHint ? String(yearHint) : '',
    period_start_date: '',
    period_end_date: '',
    period_key: yearHint ? String(yearHint) : '',
    rank: '',
    period_gross_local: semantics === 'market_chart_topline' ? weekend : '',
    cumulative_gross_local: semantics === 'title_cumulative_total' ? gross : gross,
    currency: 'USD',
    admissions_actual: '',
    work_id: '',
    distributor: '',
    notes: note + (weekCount ? ('; weeks=' + weekCount) : ''),
    raw_payload_json: JSON.stringify(hit)
  });
}

function buildAcquisitionWarnings_(query, rows, statuses, releaseMarkets, match, anomalies) {
  var warnings = [];
  var titleRows = rows.filter(function(r) { return r.record_scope === 'title'; });
  var weeklyMarkets = unique_(titleRows.filter(function(r) { return r.record_semantics === 'title_period_gross'; }).map(function(r) { return r.country_code; }).filter(Boolean));
  var perfMarkets = unique_(titleRows.map(function(r) { return r.country_code; }).filter(Boolean));
  var releaseOnly = releaseMarkets.filter(function(code) { return perfMarkets.indexOf(code) < 0; });
  var sources = unique_(titleRows.map(function(r) { return r.source_name; }));
  var failed = (statuses || []).filter(function(s) { return s.status === 'ERROR'; });

  if (!rows.length) warnings.push('No local or live evidence found for this query. This should be treated as retrieval failure, not proof of no market performance.');
  if (!match || !match.film_id) warnings.push('Canonical identity is not fully locked. Live resolution may be usable, but the workbook master identity still needs confirmation.');
  if (weeklyMarkets.length < 3) warnings.push('Weekly title-performance evidence exists in fewer than 3 target markets. Cross-market comparability remains weak.');
  if (sources.length <= 1 && titleRows.length) warnings.push('Title-level evidence is concentrated in a single source. Treat the read as provisional until another source confirms key markets.');
  if (releaseOnly.length) warnings.push('Released markets exceed performance markets: ' + releaseOnly.join(', ') + ' are released but currently lack title-performance evidence in this workbook.');
  if (rows.some(function(r) { return r.source_name === 'BOXOFFICEMOJO_TITLE'; }) && !rows.some(function(r) { return r.country_code === 'AE' && String(r.source_name).indexOf('BOXOFFICEMOJO') === 0; })) {
    warnings.push('Box Office Mojo resolved but did not convert into UAE-ready evidence. This is a material coverage gap for acquisition use.');
  }
  if (rows.some(function(r) { return r.approx_ticket_estimate && ['low', 'medium'].indexOf(String(r.ticket_estimate_confidence || '').toLowerCase()) >= 0; })) {
    warnings.push('Ticket estimates are approximate and should not be treated as decision-grade demand KPIs.');
  }
  if (failed.length) warnings.push('One or more live source fetches failed: ' + failed.map(function(x) { return x.source; }).join(', '));
  (anomalies || []).slice(0, 4).forEach(function(a) { warnings.push(a); });
  return unique_(warnings);
}

function buildAcquisitionSummary_(rows, releaseMarkets, match, statuses) {
  var titleRows = rows.filter(function(r) { return r.record_scope === 'title'; });
  var weeklyRows = titleRows.filter(function(r) { return r.record_semantics === 'title_period_gross'; });
  var cumulativeRows = titleRows.filter(function(r) { return r.record_semantics === 'title_cumulative_total'; });
  var weeklyMarkets = unique_(weeklyRows.map(function(r) { return r.country_code; }).filter(Boolean));
  var perfMarkets = unique_(titleRows.map(function(r) { return r.country_code; }).filter(Boolean));
  var releaseCodes = unique_((releaseMarkets || []).map(function(x) { return x.country_code; }).filter(Boolean));
  var missing = releaseCodes.filter(function(code) { return perfMarkets.indexOf(code) < 0; });
  var best = rankBestMarkets_(weeklyRows, cumulativeRows).slice(0, 3);
  var sourceCounts = {};
  titleRows.forEach(function(r) { sourceCounts[r.source_name] = (sourceCounts[r.source_name] || 0) + 1; });
  var topSource = Object.keys(sourceCounts).sort(function(a, b) { return sourceCounts[b] - sourceCounts[a]; })[0] || '';
  var totalSources = Object.keys(sourceCounts).length;

  var strength = 'Weak';
  if (weeklyMarkets.length >= 3 && totalSources >= 2) strength = 'Strong';
  else if (weeklyMarkets.length >= 2) strength = 'Moderate';
  else if (titleRows.length) strength = 'Limited';

  var take = 'Insufficient evidence for a confident acquisition read.';
  if (weeklyRows.length) {
    var trend = summarizeTrendByTopMarket_(weeklyRows);
    take = trend || ('Best-supported markets are ' + (best.length ? best.join(', ') : 'unclear') + ', but cross-market evidence is still incomplete.');
  } else if (cumulativeRows.length) {
    take = 'Only cumulative or partial title evidence is available. This is useful for sizing but weak for run-quality analysis.';
  }

  return {
    matched_film: (match && match.title) || (titleRows[0] && (titleRows[0].canonical_title || titleRows[0].film_title_raw)) || '',
    match_confidence: (match && match.score) || '',
    identity_status: (match && match.film_id) ? 'matched' : 'unmatched',
    release_year: (match && match.release_year) || (titleRows[0] && (titleRows[0].release_year || titleRows[0].release_year_hint)) || '',
    evidence_strength: strength,
    best_markets: best.join(', ') || 'None',
    missing_priority_markets: missing.join(', ') || 'None',
    weekly_markets: weeklyMarkets.join(', ') || 'None',
    source_concentration: totalSources ? (topSource + ' (' + sourceCounts[topSource] + ' rows, ' + totalSources + ' source' + (totalSources > 1 ? 's' : '') + ')') : 'None',
    analyst_take: take
  };
}

function rankBestMarkets_(weeklyRows, cumulativeRows) {
  var by = {};
  weeklyRows.forEach(function(r) {
    if (!by[r.country_code]) by[r.country_code] = { weeks: 0, peak: 0, latestTotal: 0, country: r.country };
    by[r.country_code].weeks += 1;
    by[r.country_code].peak = Math.max(by[r.country_code].peak, Number(r.period_gross_local || 0));
    by[r.country_code].latestTotal = Math.max(by[r.country_code].latestTotal, Number(r.cumulative_gross_local || 0));
  });
  cumulativeRows.forEach(function(r) {
    if (!by[r.country_code]) by[r.country_code] = { weeks: 0, peak: 0, latestTotal: 0, country: r.country };
    by[r.country_code].latestTotal = Math.max(by[r.country_code].latestTotal, Number(r.cumulative_gross_local || 0));
  });
  return Object.keys(by).sort(function(a, b) {
    var sa = by[a].weeks * 100000000 + by[a].latestTotal + by[a].peak;
    var sb = by[b].weeks * 100000000 + by[b].latestTotal + by[b].peak;
    return sb - sa;
  }).map(function(code) { return by[code].country || CONFIG.MARKETS[code].country || code; });
}

function summarizeTrendByTopMarket_(weeklyRows) {
  var by = {};
  weeklyRows.forEach(function(r) {
    if (!by[r.country_code]) by[r.country_code] = [];
    by[r.country_code].push(r);
  });
  var bestCode = Object.keys(by).sort(function(a, b) { return by[b].length - by[a].length; })[0];
  if (!bestCode) return '';
  var rows = by[bestCode].slice().sort(function(a, b) {
    return String(a.period_key || a.period_start_date || '').localeCompare(String(b.period_key || b.period_start_date || ''));
  });
  if (rows.length < 2) return 'Only one weekly point is available in ' + (CONFIG.MARKETS[bestCode] ? CONFIG.MARKETS[bestCode].country : bestCode) + '.';
  var first = Number(rows[0].period_gross_local || 0);
  var peak = 0;
  var peakIndex = 0;
  rows.forEach(function(r, idx) {
    var g = Number(r.period_gross_local || 0);
    if (g > peak) { peak = g; peakIndex = idx; }
  });
  var last = Number(rows[rows.length - 1].period_gross_local || 0);
  var drop = peak ? Math.round((1 - (last / peak)) * 100) : '';
  var prefix = CONFIG.MARKETS[bestCode] ? CONFIG.MARKETS[bestCode].country : bestCode;
  return prefix + ' shows ' + rows.length + ' tracked weeks, a peak weekly gross of ' + formatNumberShort_(peak) + ', and a late-run decline of about ' + drop + '% from peak to latest observed week.';
}

function buildMarketCoverageV2_(rows, releaseMarkets) {
  var byMarket = {};
  rows.forEach(function(r) {
    if (!byMarket[r.country_code]) byMarket[r.country_code] = [];
    byMarket[r.country_code].push(r);
  });
  var releaseSet = {};
  (releaseMarkets || []).forEach(function(x) { if (x.country_code) releaseSet[x.country_code] = true; });
  var out = [];
  Object.keys(CONFIG.MARKETS).forEach(function(code) {
    var list = byMarket[code] || [];
    var score = computeCoverageScore_(list, !!releaseSet[code]);
    if (!list.length) {
      out.push([CONFIG.MARKETS[code].country, score, scoreLabel_(score), '', '', '', 0, releaseSet[code] ? 'released_only' : '', '']);
      return;
    }
    var perf = list.filter(function(r) { return r.record_scope === 'title' && r.record_semantics === 'title_period_gross'; });
    var strongest = perf.length ? 'Title weekly performance' : (list.some(function(r) { return r.record_semantics === 'title_cumulative_total'; }) ? 'Cumulative total only' : 'Chart mention only');
    var latest = list.slice().sort(sortReconRowsForLookup_)[0];
    out.push([
      latest.country || CONFIG.MARKETS[code].country,
      score,
      strongest,
      latest.period_key || latest.period_label_raw || '',
      latest.period_gross_local || '',
      latest.cumulative_gross_local || '',
      list.length,
      latest.freshness_status,
      latest.source_name
    ]);
  });
  return out;
}

function computeCoverageScore_(list, releasedOnly) {
  if (!list || !list.length) return releasedOnly ? 1 : 0;
  var hasChart = list.some(function(r) { return r.record_semantics === 'market_chart_topline'; });
  var hasCum = list.some(function(r) { return r.record_semantics === 'title_cumulative_total'; });
  var weekly = list.filter(function(r) { return r.record_semantics === 'title_period_gross'; });
  var sources = unique_(list.map(function(r) { return r.source_name; }));
  if (weekly.length && sources.length >= 2) return 5;
  if (weekly.length) return 4;
  if (hasCum) return 3;
  if (hasChart) return 2;
  return releasedOnly ? 1 : 0;
}

function scoreLabel_(score) {
  var map = { 0: 'No evidence', 1: 'Release metadata only', 2: 'Chart signal only', 3: 'Cumulative only', 4: 'Weekly title evidence', 5: 'Multi-source weekly evidence' };
  return map[score] || String(score);
}

function buildComparablePerformanceRowsV2_(rows) {
  return rows
    .filter(function(r) { return r.record_scope === 'title' && (r.record_semantics === 'title_period_gross' || r.record_semantics === 'title_cumulative_total'); })
    .sort(sortReconRowsForLookup_)
    .map(function(r) {
      return [
        r.country,
        r.record_granularity,
        r.period_key || r.period_label_raw || 'lifetime',
        r.period_gross_local || '',
        r.currency || '',
        r.cumulative_gross_local || '',
        r.approx_ticket_estimate || '',
        r.ticket_estimate_confidence || '',
        r.source_name,
        r.freshness_status,
        evidenceGrade_(r)
      ];
    });
}

function buildChartRowsV2_(rows) {
  return rows
    .filter(function(r) { return r.record_semantics === 'market_chart_topline'; })
    .sort(sortReconRowsForLookup_)
    .map(function(r) {
      var metric = r.period_gross_local || r.cumulative_gross_local || '';
      return [r.country, r.period_key || r.period_label_raw, r.canonical_title || r.film_title_raw || '', metric, r.currency || '', r.source_name, r.freshness_status];
    });
}

function evidenceGrade_(r) {
  if (r.record_scope === 'title' && r.record_semantics === 'title_period_gross' && String(r.source_confidence || '').toLowerCase() !== 'low') return 'usable';
  if (r.record_scope === 'title' && r.record_semantics === 'title_cumulative_total') return 'directional';
  return 'weak';
}

function buildSourceCoverageMatrix_(rows, statuses) {
  var bySource = {};
  rows.forEach(function(r) {
    var key = r.source_name;
    if (!bySource[key]) bySource[key] = { markets: {}, rows: 0, types: {} };
    bySource[key].rows += 1;
    if (r.country_code) bySource[key].markets[r.country_code] = true;
    bySource[key].types[r.record_semantics] = true;
  });
  var out = Object.keys(bySource).sort().map(function(source) {
    var x = bySource[source];
    var type = Object.keys(x.types).join(', ');
    var markets = Object.keys(x.markets).join(', ');
    var notes = (statuses || []).filter(function(s) { return String(s.source || '').indexOf(source) === 0; }).map(function(s) { return s.status + ': ' + s.notes; }).slice(0, 2).join(' | ');
    return [source, type, markets, x.rows, notes];
  });
  if (!out.length && statuses && statuses.length) {
    out = statuses.map(function(s) { return [s.source, '', '', s.rows, s.status + ': ' + s.notes]; });
  }
  return out;
}

function promoteLiveCanonicalFilm_(ss, query, rows, releaseMarkets) {
  var titleRows = rows.filter(function(r) { return r.record_scope === 'title'; });
  if (!titleRows.length) return null;
  var candidates = {};
  titleRows.forEach(function(r) {
    var key = normalizeLatinTitle_(r.film_title_raw || r.canonical_title || '') + '|' + normalizeArabicTitle_(r.film_title_ar_raw || r.canonical_title_ar || '') + '|' + (r.release_year || r.release_year_hint || '');
    if (!candidates[key]) {
      candidates[key] = {
        title_en: r.film_title_raw || r.canonical_title || query,
        title_ar: r.film_title_ar_raw || r.canonical_title_ar || '',
        release_year: r.release_year || r.release_year_hint || '',
        score: 0,
        count: 0
      };
    }
    candidates[key].score += Number(r.match_confidence || 0) + Number(r.source_confidence || 0);
    candidates[key].count += 1;
  });
  var bestKey = Object.keys(candidates).sort(function(a, b) { return (candidates[b].score / candidates[b].count) - (candidates[a].score / candidates[a].count); })[0];
  if (!bestKey) return null;
  var best = candidates[bestKey];
  var avgScore = best.score / best.count;
  if (avgScore < 1.2) return null;

  var filmsSheet = ss.getSheetByName(CONFIG.SHEETS.FILMS);
  var aliasesSheet = ss.getSheetByName(CONFIG.SHEETS.ALIASES);
  var films = readSheetObjects_(filmsSheet);
  var aliases = readSheetObjects_(aliasesSheet);

  var existing = films.find(function(f) {
    return normalizeLatinTitle_(f.canonical_title || '') === normalizeLatinTitle_(best.title_en || '') && String(f.release_year || '') === String(best.release_year || '');
  });

  var filmId = existing ? existing.film_id : ('film_' + Utilities.getUuid().replace(/-/g, '').slice(0, 12));
  var now = isoNow_();
  var filmObj = {
    film_id: filmId,
    canonical_title: best.title_en || query,
    canonical_title_ar: best.title_ar || '',
    release_year: best.release_year || '',
    identity_confidence: Math.min(0.99, avgScore / 2),
    metadata_source: 'live_lookup_promotion',
    created_at: existing ? existing.created_at : now,
    updated_at: now
  };

  if (existing) {
    rewriteFilmRow_(filmsSheet, filmObj);
  } else {
    appendObjects_(filmsSheet, SCHEMAS.FILMS, [filmObj]);
  }

  var aliasTexts = unique_([query, best.title_en, best.title_ar].filter(Boolean));
  var aliasExisting = {};
  aliases.forEach(function(a) { aliasExisting[(a.film_id || '') + '|' + (a.normalized_alias || '')] = true; });
  var aliasRows = [];
  aliasTexts.forEach(function(txt) {
    var norm = /[\u0600-\u06FF]/.test(txt) ? normalizeArabicTitle_(txt) : normalizeLatinTitle_(txt);
    var key = filmId + '|' + norm;
    if (norm && !aliasExisting[key]) {
      aliasRows.push({
        alias_id: 'alias_' + Utilities.getUuid().replace(/-/g, '').slice(0, 12),
        film_id: filmId,
        alias_text: txt,
        normalized_alias: norm,
        alias_language: /[\u0600-\u06FF]/.test(txt) ? 'ar' : 'en',
        alias_type: 'live_discovery',
        confidence: Math.min(0.99, avgScore / 2),
        source: 'live_lookup',
        needs_review: '',
        created_at: now
      });
    }
  });
  if (aliasRows.length) appendObjects_(aliasesSheet, SCHEMAS.ALIASES, aliasRows);
  return filmObj;
}

function rewriteFilmRow_(sheet, filmObj) {
  var rows = readSheetObjects_(sheet);
  var updated = false;
  rows = rows.map(function(r) {
    if (r.film_id === filmObj.film_id) {
      updated = true;
      return filmObj;
    }
    return r;
  });
  if (!updated) rows.push(filmObj);
  rewriteSheetObjects_(sheet, SCHEMAS.FILMS, rows);
}

function appendReviewItemsForLookupAnomalies_(ss, query, rows, statuses, anomalies) {
  var reviewSheet = ss.getSheetByName(CONFIG.SHEETS.REVIEW);
  var items = [];
  var now = isoNow_();
  (anomalies || []).slice(0, 8).forEach(function(msg) {
    items.push({
      review_id: 'rvw_' + Utilities.getUuid().replace(/-/g, '').slice(0, 12),
      created_at: now,
      review_type: 'parser_anomaly',
      status: 'open',
      severity: 'medium',
      film_title_raw: query,
      film_id_candidate: rows[0] ? (rows[0].film_id || '') : '',
      country: '',
      period_key: '',
      source_name: '',
      source_url: '',
      details_json: JSON.stringify({ message: msg }),
      analyst_notes: ''
    });
  });
  var failed = (statuses || []).filter(function(s) { return s.status === 'ERROR'; });
  failed.slice(0, 6).forEach(function(s) {
    items.push({
      review_id: 'rvw_' + Utilities.getUuid().replace(/-/g, '').slice(0, 12),
      created_at: now,
      review_type: 'source_conflict',
      status: 'open',
      severity: 'high',
      film_title_raw: query,
      film_id_candidate: rows[0] ? (rows[0].film_id || '') : '',
      country: '',
      period_key: '',
      source_name: s.source,
      source_url: '',
      details_json: JSON.stringify(s),
      analyst_notes: ''
    });
  });
  if (items.length) appendObjects_(reviewSheet, SCHEMAS.REVIEW, items);
}

function styleBlockHeader_(range, color) {
  range.setFontWeight('bold').setBackground(color).setFontColor('#111827');
}

function styleLabelValueBlock_(sheet, startRow, numRows, numCols) {
  if (numRows <= 0) return;
  sheet.getRange(startRow, 1, numRows, 1).setFontWeight('bold').setBackground('#F9FAFB');
}

function formatNumberShort_(n) {
  n = Number(n || 0);
  if (!n) return '0';
  if (Math.abs(n) >= 1000000) return (Math.round((n / 1000000) * 10) / 10) + 'M';
  if (Math.abs(n) >= 1000) return (Math.round((n / 1000) * 10) / 10) + 'K';
  return String(Math.round(n));
}

function escapeRegex_(s) {
  return String(s || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// ============================================================
// SECTION 15: V5.0 QUEUED ENRICHMENT OVERRIDES
// ============================================================

/**
 * Goal of this patch:
 *  - guarantee stable execution inside Apps Script limits
 *  - split lookup into fast analyst retrieval + queued enrichment
 *  - preserve the single-project spreadsheet workflow
 *  - keep collecting all reachable supported-source data in stages
 */

function getLookupJobSchema_() {
  return [
    'job_id', 'title_query', 'normalized_query_en', 'normalized_query_ar', 'release_year_hint',
    'status', 'stage', 'priority', 'attempts', 'created_at', 'updated_at', 'last_error',
    'film_id', 'matched_title', 'matched_title_ar', 'matched_release_year',
    'discovered_work_ids_json', 'processed_work_ids_json',
    'discovered_released_work_ids_json', 'processed_released_work_ids_json',
    'discovered_title_urls_json', 'processed_title_urls_json',
    'discovered_release_urls_json', 'processed_release_urls_json',
    'release_markets_json', 'source_status_json', 'anomalies_json',
    'dirty_recon', 'active_lookup', 'notes'
  ];
}

function getLookupJobsSheetName_() {
  return 'Lookup_Jobs';
}

function ensureLookupJobsSheet_(ss) {
  var name = getLookupJobsSheetName_();
  var sheet = ss.getSheetByName(name);
  if (!sheet) sheet = ss.insertSheet(name);
  var schema = getLookupJobSchema_();
  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, schema.length).setValues([schema]).setFontWeight('bold');
    sheet.setFrozenRows(1);
  } else {
    var header = sheet.getRange(1, 1, 1, Math.max(sheet.getLastColumn(), schema.length)).getValues()[0];
    var mismatch = schema.some(function(h, idx) { return header[idx] !== h; });
    if (mismatch) {
      rewriteSheetObjects_(sheet, schema, readSheetObjects_(sheet));
    }
  }
  return sheet;
}

function installMenu() {
  SpreadsheetApp.getUi()
    .createMenu('🎬 MENA Intelligence')
    .addItem('Initialize Workbook', 'initializeWorkbook')
    .addItem('Search Title (Fast + Queue)', 'searchTitleFromSheet')
    .addItem('Refresh Live Lookup (Fast + Queue)', 'refreshLiveLookupFromSheet')
    .addSeparator()
    .addItem('Open Dashboard', 'openDashboard')
    .addItem('Update Dashboard', 'updateDashboardFromSheet')
    .addSeparator()
    .addItem('Process Lookup Queue', 'processLookupQueue')
    .addItem('Install Lookup Worker Trigger', 'installLookupWorkerTrigger')
    .addItem('Remove Lookup Worker Trigger', 'removeLookupWorkerTrigger')
    .addSeparator()
    .addItem('Run Pipeline', 'runPipeline')
    .addItem('Review Queue', 'openReviewQueue')
    .addItem('Test Sources', 'testSources')
    .addToUi();
}

function onOpen() {
  installMenu();
}

function initializeWorkbook() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  ensureLookupJobsSheet_(ss);

  var lookupSheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  var dashboardSheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  var existingLookupQuery = lookupSheet ? String(lookupSheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '') : '';
  var existingDashboardQuery = dashboardSheet ? String(dashboardSheet.getRange('B2').getValue() || '') : '';
  var existingDashboardCountry = dashboardSheet ? String(dashboardSheet.getRange('B3').getValue() || 'ALL') : 'ALL';

  seedSystemConfig_(ss);
  seedTicketPrices_(ss);
  setupLookupSheet_(ss, existingLookupQuery);
  setupDashboardSheet_(ss, existingDashboardQuery || existingLookupQuery, existingDashboardCountry);
  writeLookupJobBanner_(ss, null, 'Idle');
  SpreadsheetApp.getUi().alert('Workbook initialized with fast lookup, dashboard, and queued enrichment.');
}

function searchTitleFromSheet() {
  runQueuedLookupFromSheet_(false);
}

function refreshLiveLookupFromSheet() {
  runQueuedLookupFromSheet_(true);
}

function runQueuedLookupFromSheet_(forceRefresh) {
  var lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    ensureAllSheets_(ss);
    ensureLookupJobsSheet_(ss);
    var sheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
    setupLookupSheet_(ss, String(sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || ''));
    var query = String(sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '').trim();
    if (!query) {
      SpreadsheetApp.getUi().alert('Enter a title in B3.');
      return;
    }

    var job = createOrResetLookupJob_(ss, query, { forceRefresh: forceRefresh, activeLookup: true });
    writeLookupJobBanner_(ss, job, 'Queued');

    // Prime the lookup with a short bounded run so analysts see evidence quickly.
    processLookupJobBudgetById_(ss, job.job_id, 12000);

    var refreshed = getLookupJobById_(ss, job.job_id) || job;
    renderLookup_(query, false);
    writeLookupJobBanner_(ss, refreshed, refreshed.status || 'running');

    SpreadsheetApp.getActive().toast('Lookup started. Initial evidence loaded; background enrichment can continue from the queue.', 'MENA Intelligence', 6);
  } finally {
    lock.releaseLock();
  }
}

function installLookupWorkerTrigger() {
  removeLookupWorkerTrigger();
  ScriptApp.newTrigger('processLookupQueue')
    .timeBased()
    .everyMinutes(1)
    .create();
  SpreadsheetApp.getActive().toast('Lookup worker trigger installed.', 'MENA Intelligence', 5);
}

function removeLookupWorkerTrigger() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'processLookupQueue') ScriptApp.deleteTrigger(t);
  });
}

function processLookupQueue() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return;
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    ensureAllSheets_(ss);
    ensureLookupJobsSheet_(ss);

    var jobs = getActiveLookupJobs_(ss);
    if (!jobs.length) return;

    // Highest priority first, oldest first.
    jobs.sort(function(a, b) {
      return (Number(b.priority || 0) - Number(a.priority || 0)) || String(a.created_at || '').localeCompare(String(b.created_at || ''));
    });

    var deadline = Date.now() + 45000;
    for (var i = 0; i < jobs.length && Date.now() < deadline; i++) {
      processLookupJobBudgetById_(ss, jobs[i].job_id, Math.max(5000, deadline - Date.now() - 1000));
    }
  } finally {
    lock.releaseLock();
  }
}

function processLookupJobBudgetById_(ss, jobId, budgetMs) {
  var start = Date.now();
  var dirtyRecon = false;
  var progressed = false;
  while ((Date.now() - start) < budgetMs) {
    var job = getLookupJobById_(ss, jobId);
    if (!job) break;
    if (job.status === 'complete' || job.status === 'failed' || job.stage === 'COMPLETE') break;
    var step = processSingleLookupJobStage_(ss, job);
    progressed = progressed || step.progressed;
    dirtyRecon = dirtyRecon || step.dirtyRecon;
    if (!step.progressed) break;
    if ((Date.now() - start) > (budgetMs - 2500)) break;
  }

  if (dirtyRecon) {
    rebuildReconciledEvidence_(ss);
  }

  var finalJob = getLookupJobById_(ss, jobId);
  if (finalJob && finalJob.status === 'complete') {
    rebuildReconciledEvidence_(ss);
  }

  if (finalJob && finalJob.active_lookup === 'yes') {
    var sheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
    var currentQuery = String(sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '').trim();
    if (normalizeLatinTitle_(currentQuery) === normalizeLatinTitle_(finalJob.title_query)) {
      renderLookup_(currentQuery, false);
      writeLookupJobBanner_(ss, getLookupJobById_(ss, jobId), (finalJob.status || '').toUpperCase());
    }
  }
}

function processSingleLookupJobStage_(ss, job) {
  var stage = job.stage || 'DISCOVERY';
  var query = job.title_query;
  var yearHint = job.release_year_hint || extractYearHintFromQuery_(query) || '';
  var dirtyRecon = false;
  var progressed = false;

  job.status = 'running';
  job.attempts = Number(job.attempts || 0) + 1;
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);

  try {
    if (stage === 'DISCOVERY') {
      progressed = lookupStageDiscovery_(ss, job, query, yearHint);
    } else if (stage === 'ELCINEMA_TITLE') {
      var s1 = lookupStageElCinemaTitle_(ss, job, query, yearHint);
      progressed = s1.progressed; dirtyRecon = dirtyRecon || s1.dirtyRecon;
    } else if (stage === 'ELCINEMA_RELEASED') {
      progressed = lookupStageElCinemaReleased_(ss, job);
    } else if (stage === 'BOM_TITLE') {
      var s2 = lookupStageBomTitle_(ss, job, query, yearHint);
      progressed = s2.progressed; dirtyRecon = dirtyRecon || s2.dirtyRecon;
    } else if (stage === 'BOM_RELEASE') {
      var s3 = lookupStageBomRelease_(ss, job, query, yearHint);
      progressed = s3.progressed; dirtyRecon = dirtyRecon || s3.dirtyRecon;
    } else if (stage === 'SUPPLEMENTAL_CHARTS') {
      var s4 = lookupStageSupplementalCharts_(ss, job, query, yearHint);
      progressed = s4.progressed; dirtyRecon = dirtyRecon || s4.dirtyRecon;
    } else if (stage === 'FINALIZE') {
      finalizeLookupJob_(ss, job);
      progressed = true; dirtyRecon = true;
    }
  } catch (e) {
    appendJobSourceStatus_(ss, job, {
      source: 'JOB_STAGE:' + stage,
      status: 'ERROR',
      rows: 0,
      notes: e.message
    });
    appendJobAnomaly_(ss, job, 'Stage ' + stage + ' failed: ' + e.message);
    job.last_error = e.message;
    job.status = 'failed';
    job.updated_at = isoNow_();
    upsertLookupJob_(ss, job);
    enqueueReview_(ss, {
      review_type: 'parser_anomaly',
      status: 'open',
      severity: 'high',
      film_title_raw: query,
      film_id_candidate: job.film_id || '',
      country: '',
      period_key: '',
      source_name: 'LOOKUP_QUEUE',
      source_url: '',
      details_json: JSON.stringify({ stage: stage, error: e.message, job_id: job.job_id }),
      analyst_notes: ''
    });
  }

  return { progressed: progressed, dirtyRecon: dirtyRecon };
}

function lookupStageDiscovery_(ss, job, query, yearHint) {
  var eCandidates = [];
  var mCandidates = [];

  try {
    eCandidates = discoverElCinemaWorkCandidates_(query, yearHint) || [];
  } catch (e) {
    appendJobSourceStatus_(ss, job, { source: 'ELCINEMA_DISCOVERY', status: 'ERROR', rows: 0, notes: e.message });
    appendJobAnomaly_(ss, job, 'elCinema discovery failed: ' + e.message);
  }

  try {
    mCandidates = discoverBoxOfficeMojoCandidates_(query, yearHint) || [];
  } catch (e2) {
    appendJobSourceStatus_(ss, job, { source: 'BOXOFFICEMOJO_DISCOVERY', status: 'ERROR', rows: 0, notes: e2.message });
    appendJobAnomaly_(ss, job, 'Box Office Mojo discovery failed: ' + e2.message);
  }

  var workIds = unique_(eCandidates.map(function(c) { return c.work_id; }).filter(Boolean));
  var titleUrls = unique_(mCandidates.map(function(c) { return c.title_url; }).filter(Boolean));
  var releaseUrls = unique_(mCandidates.map(function(c) { return c.release_url; }).filter(Boolean));

  job.discovered_work_ids_json = JSON.stringify(workIds);
  job.processed_work_ids_json = job.processed_work_ids_json || '[]';
  job.discovered_released_work_ids_json = JSON.stringify(workIds);
  job.processed_released_work_ids_json = job.processed_released_work_ids_json || '[]';
  job.discovered_title_urls_json = JSON.stringify(titleUrls);
  job.processed_title_urls_json = job.processed_title_urls_json || '[]';
  job.discovered_release_urls_json = JSON.stringify(releaseUrls);
  job.processed_release_urls_json = job.processed_release_urls_json || '[]';
  job.updated_at = isoNow_();
  job.stage = workIds.length ? 'ELCINEMA_TITLE' : (titleUrls.length ? 'BOM_TITLE' : (releaseUrls.length ? 'BOM_RELEASE' : 'SUPPLEMENTAL_CHARTS'));
  upsertLookupJob_(ss, job);

  appendJobSourceStatus_(ss, job, {
    source: 'ELCINEMA_DISCOVERY',
    status: 'OK',
    rows: workIds.length,
    notes: workIds.length ? 'Resolved elCinema work candidates' : 'No elCinema work candidates found'
  });
  appendJobSourceStatus_(ss, job, {
    source: 'BOXOFFICEMOJO_DISCOVERY',
    status: 'OK',
    rows: titleUrls.length + releaseUrls.length,
    notes: (titleUrls.length || releaseUrls.length) ? 'Resolved Box Office Mojo title/release candidates' : 'No Box Office Mojo candidates found'
  });

  return true;
}

function lookupStageElCinemaTitle_(ss, job, query, yearHint) {
  var discovered = parseJsonArray_(job.discovered_work_ids_json);
  var processed = parseJsonArray_(job.processed_work_ids_json);
  var next = discovered.filter(function(id) { return processed.indexOf(id) < 0; })[0];
  if (!next) {
    job.stage = 'ELCINEMA_RELEASED';
    job.updated_at = isoNow_();
    upsertLookupJob_(ss, job);
    return { progressed: true, dirtyRecon: false };
  }

  var rows = [];
  try {
    rows = (fetchElCinemaTitleBoxOffice_(next) || []).filter(function(r) {
      return titleMatchesQuery_(query, r.film_title_raw, r.film_title_ar_raw || '', r.release_year_hint, yearHint);
    }).map(function(r) {
      r.match_confidence = Math.max(Number(r.match_confidence || 0), 0.92);
      return r;
    });
    appendJobSourceStatus_(ss, job, {
      source: 'ELCINEMA_TITLE_BOXOFFICE:' + next,
      status: 'OK',
      rows: rows.length,
      notes: rows.length ? 'Fetched title-specific weekly history' : 'No weekly history rows found'
    });
    if (!rows.length) appendJobAnomaly_(ss, job, 'elCinema title box office resolved but yielded zero usable rows: ' + next);
  } catch (e) {
    appendJobSourceStatus_(ss, job, { source: 'ELCINEMA_TITLE_BOXOFFICE:' + next, status: 'ERROR', rows: 0, notes: e.message });
    appendJobAnomaly_(ss, job, 'elCinema title fetch failed for ' + next + ': ' + e.message);
  }

  if (rows.length) {
    persistRawRecordsForJob_(ss, job, rows);
    autopromoteFilmFromRows_(ss, job, rows);
    job.dirty_recon = 'yes';
  }

  processed.push(next);
  job.processed_work_ids_json = JSON.stringify(unique_(processed));
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
  return { progressed: true, dirtyRecon: rows.length > 0 };
}

function lookupStageElCinemaReleased_(ss, job) {
  var discovered = parseJsonArray_(job.discovered_released_work_ids_json);
  var processed = parseJsonArray_(job.processed_released_work_ids_json);
  var releaseMarkets = parseJsonArray_(job.release_markets_json);
  var next = discovered.filter(function(id) { return processed.indexOf(id) < 0; })[0];
  if (!next) {
    job.stage = parseJsonArray_(job.discovered_title_urls_json).length ? 'BOM_TITLE' : (parseJsonArray_(job.discovered_release_urls_json).length ? 'BOM_RELEASE' : 'SUPPLEMENTAL_CHARTS');
    job.updated_at = isoNow_();
    upsertLookupJob_(ss, job);
    return true;
  }

  try {
    var released = fetchElCinemaReleasedMarkets_(next) || [];
    releaseMarkets = releaseMarkets.concat(released.map(function(r) { return r.country_code; }).filter(Boolean));
    job.release_markets_json = JSON.stringify(unique_(releaseMarkets));
    appendJobSourceStatus_(ss, job, {
      source: 'ELCINEMA_RELEASED:' + next,
      status: 'OK',
      rows: released.length,
      notes: released.length ? ('Release dates found for: ' + unique_(released.map(function(r) { return r.country_code; })).join(', ')) : 'No release markets found'
    });
  } catch (e) {
    appendJobSourceStatus_(ss, job, { source: 'ELCINEMA_RELEASED:' + next, status: 'ERROR', rows: 0, notes: e.message });
    appendJobAnomaly_(ss, job, 'elCinema release dates failed for ' + next + ': ' + e.message);
  }

  processed.push(next);
  job.processed_released_work_ids_json = JSON.stringify(unique_(processed));
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
  return true;
}

function lookupStageBomTitle_(ss, job, query, yearHint) {
  var discovered = parseJsonArray_(job.discovered_title_urls_json);
  var processed = parseJsonArray_(job.processed_title_urls_json);
  var discoveredReleaseUrls = parseJsonArray_(job.discovered_release_urls_json);
  var next = discovered.filter(function(url) { return processed.indexOf(url) < 0; })[0];
  if (!next) {
    job.stage = 'BOM_RELEASE';
    job.updated_at = isoNow_();
    upsertLookupJob_(ss, job);
    return { progressed: true, dirtyRecon: false };
  }

  var titleRows = [];
  try {
    var titleInfo = fetchBoxOfficeMojoTitleCandidate_(next);
    titleRows = (buildBoxOfficeMojoTitleSummaryRows_(titleInfo) || []).filter(function(r) {
      return titleMatchesQuery_(query, r.film_title_raw, r.film_title_ar_raw || '', r.release_year_hint, yearHint);
    });
    discoveredReleaseUrls = unique_(discoveredReleaseUrls.concat((titleInfo.release_urls || []).filter(Boolean)));
    job.discovered_release_urls_json = JSON.stringify(discoveredReleaseUrls);
    appendJobSourceStatus_(ss, job, {
      source: 'BOXOFFICEMOJO_TITLE',
      status: 'OK',
      rows: titleRows.length,
      notes: titleRows.length ? 'Title page yielded direct area/summary evidence' : 'Title page resolved but yielded no usable rows'
    });
    if (!titleRows.length) appendJobAnomaly_(ss, job, 'Box Office Mojo title page resolved with zero usable rows: ' + next);
  } catch (e) {
    appendJobSourceStatus_(ss, job, { source: 'BOXOFFICEMOJO_TITLE', status: 'ERROR', rows: 0, notes: e.message });
    appendJobAnomaly_(ss, job, 'Box Office Mojo title page failed for ' + next + ': ' + e.message);
  }

  if (titleRows.length) {
    persistRawRecordsForJob_(ss, job, titleRows);
    autopromoteFilmFromRows_(ss, job, titleRows);
    job.dirty_recon = 'yes';
  }

  processed.push(next);
  job.processed_title_urls_json = JSON.stringify(unique_(processed));
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
  return { progressed: true, dirtyRecon: titleRows.length > 0 };
}

function lookupStageBomRelease_(ss, job, query, yearHint) {
  var discovered = parseJsonArray_(job.discovered_release_urls_json);
  var processed = parseJsonArray_(job.processed_release_urls_json);
  var next = discovered.filter(function(url) { return processed.indexOf(url) < 0; })[0];
  if (!next) {
    job.stage = 'SUPPLEMENTAL_CHARTS';
    job.updated_at = isoNow_();
    upsertLookupJob_(ss, job);
    return { progressed: true, dirtyRecon: false };
  }

  var relRows = [];
  try {
    relRows = (fetchBoxOfficeMojoReleaseEvidence_(next, query, yearHint) || []).filter(function(r) {
      return titleMatchesQuery_(query, r.film_title_raw, r.film_title_ar_raw || '', r.release_year_hint, yearHint);
    });
    appendJobSourceStatus_(ss, job, {
      source: 'BOXOFFICEMOJO_RELEASE',
      status: 'OK',
      rows: relRows.length,
      notes: relRows.length ? 'Fetched territory-specific release evidence' : 'Release page resolved but yielded no usable rows'
    });
    if (!relRows.length) appendJobAnomaly_(ss, job, 'Box Office Mojo release page resolved with zero usable rows: ' + next);
  } catch (e) {
    appendJobSourceStatus_(ss, job, { source: 'BOXOFFICEMOJO_RELEASE', status: 'ERROR', rows: 0, notes: e.message });
    appendJobAnomaly_(ss, job, 'Box Office Mojo release page failed for ' + next + ': ' + e.message);
  }

  if (relRows.length) {
    persistRawRecordsForJob_(ss, job, relRows);
    autopromoteFilmFromRows_(ss, job, relRows);
    job.dirty_recon = 'yes';
  }

  processed.push(next);
  job.processed_release_urls_json = JSON.stringify(unique_(processed));
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
  return { progressed: true, dirtyRecon: relRows.length > 0 };
}

function lookupStageSupplementalCharts_(ss, job, query, yearHint) {
  var rows = [];
  try {
    var chart = (fetchElCinemaCurrentChart_() || []).filter(function(r) {
      return titleMatchesQuery_(query, r.film_title_raw, r.film_title_ar_raw || '', r.release_year_hint, yearHint);
    }).map(function(r) {
      r.match_confidence = Math.max(Number(r.match_confidence || 0), 0.80);
      return r;
    });
    rows = rows.concat(chart);
    appendJobSourceStatus_(ss, job, { source: 'ELCINEMA_CHART', status: 'OK', rows: chart.length, notes: 'Current Egypt chart title hits (supplemental only)' });
  } catch (e) {
    appendJobSourceStatus_(ss, job, { source: 'ELCINEMA_CHART', status: 'ERROR', rows: 0, notes: e.message });
  }

  try {
    var intl = (fetchBoxOfficeMojoIntlLatest_() || []).filter(function(r) {
      return titleMatchesQuery_(query, r.film_title_raw, '', r.release_year_hint, yearHint);
    }).map(function(r) {
      r.match_confidence = Math.max(Number(r.match_confidence || 0), 0.70);
      return r;
    });
    rows = rows.concat(intl);
    appendJobSourceStatus_(ss, job, { source: 'BOXOFFICEMOJO_INTL', status: 'OK', rows: intl.length, notes: 'Latest international weekend chart signal (supplemental only)' });
  } catch (e2) {
    appendJobSourceStatus_(ss, job, { source: 'BOXOFFICEMOJO_INTL', status: 'ERROR', rows: 0, notes: e2.message });
  }

  rows = dedupRawRecords_(rows);
  if (rows.length) {
    persistRawRecordsForJob_(ss, job, rows);
    autopromoteFilmFromRows_(ss, job, rows);
    job.dirty_recon = 'yes';
  }

  job.stage = 'FINALIZE';
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
  return { progressed: true, dirtyRecon: rows.length > 0 };
}

function finalizeLookupJob_(ss, job) {
  var statusList = parseJsonArray_(job.source_status_json);
  var anomalies = parseJsonArray_(job.anomalies_json);
  if (anomalies.length) {
    enqueueReview_(ss, {
      review_type: 'parser_anomaly',
      status: 'open',
      severity: 'medium',
      film_title_raw: job.title_query,
      film_id_candidate: job.film_id || '',
      country: '',
      period_key: '',
      source_name: 'LOOKUP_QUEUE',
      source_url: '',
      details_json: JSON.stringify({ job_id: job.job_id, anomalies: anomalies, statuses: statusList }),
      analyst_notes: ''
    });
  }
  job.status = 'complete';
  job.stage = 'COMPLETE';
  job.dirty_recon = 'no';
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
}

function rebuildReconciledEvidence_(ss) {
  var ctx = createRunContext_();
  var rawRows = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RAW));
  var reconRows = reconcileEvidence_(ss, rawRows, ctx);
  rewriteSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RECON), SCHEMAS.RECON, reconRows);
}

function createOrResetLookupJob_(ss, query, options) {
  options = options || {};
  var jobs = readSheetObjects_(ensureLookupJobsSheet_(ss));

  // Deactivate previous active jobs for same normalized query when forcing refresh.
  var nq = normalizeLatinTitle_(query);
  jobs.forEach(function(j) {
    if (normalizeLatinTitle_(j.title_query || '') === nq && (j.status === 'queued' || j.status === 'running') && options.forceRefresh) {
      j.active_lookup = 'no';
      j.status = 'superseded';
      j.updated_at = isoNow_();
      upsertLookupJob_(ss, j);
    }
  });

  var existing = readSheetObjects_(ensureLookupJobsSheet_(ss)).find(function(j) {
    return normalizeLatinTitle_(j.title_query || '') === nq && (j.status === 'queued' || j.status === 'running') && !options.forceRefresh;
  });
  if (existing) {
    existing.active_lookup = options.activeLookup ? 'yes' : (existing.active_lookup || 'no');
    existing.priority = options.forceRefresh ? 100 : (existing.priority || 50);
    existing.updated_at = isoNow_();
    upsertLookupJob_(ss, existing);
    return existing;
  }

  var job = {
    job_id: 'job_' + Utilities.getUuid().slice(0, 8),
    title_query: query,
    normalized_query_en: normalizeLatinTitle_(query),
    normalized_query_ar: normalizeArabicTitle_(query),
    release_year_hint: extractYearHintFromQuery_(query) || '',
    status: 'queued',
    stage: 'DISCOVERY',
    priority: options.forceRefresh ? 100 : 50,
    attempts: 0,
    created_at: isoNow_(),
    updated_at: isoNow_(),
    last_error: '',
    film_id: '',
    matched_title: '',
    matched_title_ar: '',
    matched_release_year: '',
    discovered_work_ids_json: '[]',
    processed_work_ids_json: '[]',
    discovered_released_work_ids_json: '[]',
    processed_released_work_ids_json: '[]',
    discovered_title_urls_json: '[]',
    processed_title_urls_json: '[]',
    discovered_release_urls_json: '[]',
    processed_release_urls_json: '[]',
    release_markets_json: '[]',
    source_status_json: '[]',
    anomalies_json: '[]',
    dirty_recon: 'no',
    active_lookup: options.activeLookup ? 'yes' : 'no',
    notes: ''
  };
  upsertLookupJob_(ss, job);
  return job;
}

function getActiveLookupJobs_(ss) {
  return readSheetObjects_(ensureLookupJobsSheet_(ss)).filter(function(j) {
    return j.status === 'queued' || j.status === 'running';
  });
}

function getLookupJobById_(ss, jobId) {
  var rows = readSheetObjects_(ensureLookupJobsSheet_(ss));
  return rows.find(function(r) { return r.job_id === jobId; }) || null;
}

function upsertLookupJob_(ss, job) {
  var sheet = ensureLookupJobsSheet_(ss);
  var schema = getLookupJobSchema_();
  var rows = readSheetObjects_(sheet);
  var idx = rows.findIndex(function(r) { return r.job_id === job.job_id; });
  if (idx >= 0) rows[idx] = Object.assign({}, rows[idx], job);
  else rows.push(job);
  rewriteSheetObjects_(sheet, schema, rows);
}

function appendJobSourceStatus_(ss, job, statusObj) {
  var list = parseJsonArray_(job.source_status_json);
  list.push({
    checked_at: isoNow_(),
    source: statusObj.source,
    status: statusObj.status,
    rows: statusObj.rows,
    notes: statusObj.notes
  });
  job.source_status_json = JSON.stringify(list);
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);

  appendObjects_(ss.getSheetByName(CONFIG.SHEETS.SOURCE_STATUS), SCHEMAS.SOURCE_STATUS, [{
    checked_at: isoNow_(),
    source_name: statusObj.source,
    country: '',
    status: statusObj.status,
    rows: statusObj.rows,
    message: statusObj.notes
  }]);
}

function appendJobAnomaly_(ss, job, message) {
  var list = parseJsonArray_(job.anomalies_json);
  list.push(message);
  job.anomalies_json = JSON.stringify(unique_(list));
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
}

function persistRawRecordsForJob_(ss, job, rows) {
  var normalized = rows.map(function(r) {
    r.run_id = job.job_id;
    return normalizeRawRecord_(r, job.job_id);
  });
  appendRawEvidence_(ss, normalized, job.job_id);
}

function autopromoteFilmFromRows_(ss, job, rows) {
  if (!rows || !rows.length) return;
  var best = rows[0];
  rows.forEach(function(r) {
    if (Number(r.match_confidence || 0) > Number(best.match_confidence || 0)) best = r;
  });
  var normalizedRows = rows.map(function(r) { return normalizeRawRecord_(r, job.job_id); });
  var ctx = createRunContext_();
  var filmId = '';
  normalizedRows.some(function(r) {
    if (r.record_scope !== 'title') return false;
    filmId = resolveFilmIdForRaw_(ss, r, ctx);
    return !!filmId;
  });
  if (!filmId) return;
  var film = findFilmById_(ss, filmId);
  if (!film) return;
  job.film_id = filmId;
  job.matched_title = film.canonical_title || best.film_title_raw || '';
  job.matched_title_ar = film.canonical_title_ar || best.film_title_ar_raw || '';
  job.matched_release_year = film.release_year || best.release_year_hint || '';
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
}

function writeLookupJobBanner_(ss, job, statusLabel) {
  var sheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  if (!sheet) return;
  var bannerRange = sheet.getRange('G1:J5');
  bannerRange.clearContent().clearFormat();
  sheet.getRange('G1').setValue('Lookup Queue Status').setFontWeight('bold').setBackground('#E5E7EB');

  if (!job) {
    sheet.getRange('G2:H4').setValues([
      ['Status', statusLabel || 'Idle'],
      ['Stage', ''],
      ['Job ID', '']
    ]);
    return;
  }

  sheet.getRange('G2:H5').setValues([
    ['Status', String(statusLabel || job.status || '').toUpperCase()],
    ['Stage', job.stage || ''],
    ['Job ID', job.job_id || ''],
    ['Last Update', job.updated_at || '']
  ]);
  sheet.getRange('I2:J5').setValues([
    ['Matched Film', job.matched_title || ''],
    ['Film ID', job.film_id || ''],
    ['Release Year', job.matched_release_year || ''],
    ['Errors', job.last_error || '']
  ]);
  styleLabelValueBlock_(sheet, 2, 4, 2, 7);
  styleLabelValueBlock_(sheet, 2, 4, 2, 9);
}

function styleLabelValueBlock_(sheet, startRow, numRows, numCols, startCol) {
  startCol = startCol || 1;
  for (var i = 0; i < numRows; i++) {
    sheet.getRange(startRow + i, startCol).setFontWeight('bold').setBackground('#F3F4F6');
  }
}

function parseJsonArray_(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  try {
    var parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch (e) {
    return [];
  }
}

// ============================================================
// SECTION 20: v5.1 HARDENING PATCH
// Purpose:
//   - restore missing normalization path used by queue stages
//   - add worker dependency preflight checks
//   - keep lookup banner and page status aligned with actual job state
//   - fail gracefully with explicit partial-retrieval messaging
// ============================================================

function normalizeRawRecord_(record, runId) {
  record = record || {};
  if (typeof buildRawRecord_ !== 'function') {
    throw new Error('buildRawRecord_ is not defined');
  }

  // Already normalized enough for RAW schema: coerce fields but preserve semantics.
  var out = {};
  Object.keys(record).forEach(function(k) { out[k] = record[k]; });

  out.run_id = runId || out.run_id || '';
  out.fetched_at = normalizeIsoDateTime_(out.fetched_at) || isoNow_();
  out.source_name = String(out.source_name || '');
  out.source_url = String(out.source_url || '');
  out.parser_version = String(out.parser_version || CONFIG.VERSION || '');
  out.parser_confidence = coerceNumberOrBlank_(out.parser_confidence, 0.6);
  out.source_entity_id = String(out.source_entity_id || '');
  out.country = String(out.country || '');
  out.country_code = normalizeMarketCode_(out.country_code || findMarketCodeByCountry_(out.country) || '');
  out.film_title_raw = String(out.film_title_raw || '');
  out.film_title_ar_raw = String(out.film_title_ar_raw || '');
  out.release_year_hint = normalizeYearHint_(out.release_year_hint);
  out.record_scope = String(out.record_scope || '');
  out.record_granularity = String(out.record_granularity || '');
  out.record_semantics = String(out.record_semantics || '');
  out.source_confidence = coerceNumberOrBlank_(out.source_confidence, 0.5);
  out.match_confidence = coerceNumberOrBlank_(out.match_confidence, 0.5);
  out.evidence_type = String(out.evidence_type || '');
  out.period_label_raw = String(out.period_label_raw || '');
  out.period_start_date = normalizeIsoDate_(out.period_start_date);
  out.period_end_date = normalizeIsoDate_(out.period_end_date);
  out.period_key = String(out.period_key || derivePeriodKeySafe_(out));
  out.rank = coerceNumberOrBlank_(out.rank, '');
  out.period_gross_local = coerceNumberOrBlank_(out.period_gross_local, '');
  out.cumulative_gross_local = coerceNumberOrBlank_(out.cumulative_gross_local, '');
  out.currency = String(out.currency || '').toUpperCase();
  out.admissions_actual = coerceNumberOrBlank_(out.admissions_actual, '');
  out.work_id = String(out.work_id || '');
  out.distributor = String(out.distributor || '');
  out.notes = String(out.notes || '');
  out.raw_payload_json = stringifyJsonSafe_(out.raw_payload_json || {});

  var built = buildRawRecord_(out);
  if (!built.raw_key) {
    built.raw_key = makeStableRawKey_(built);
  }
  // Preserve explicitly provided freshness if present, otherwise compute via builder.
  if (out.freshness_status) built.freshness_status = String(out.freshness_status);
  return built;
}

function validateLookupWorkerDependencies_() {
  var required = [
    'buildRawRecord_',
    'appendRawEvidence_',
    'reconcileEvidence_',
    'renderLookup_',
    'writeLookupJobBanner_',
    'upsertLookupJob_',
    'appendJobSourceStatus_',
    'appendJobAnomaly_',
    'autopromoteFilmFromRows_',
    'persistRawRecordsForJob_',
    'normalizeRawRecord_'
  ];
  var missing = required.filter(function(name) {
    return typeof globalThis[name] !== 'function';
  });
  if (missing.length) {
    throw new Error('Missing required helper(s): ' + missing.join(', '));
  }
}

function processSingleLookupJobStage_(ss, job) {
  validateLookupWorkerDependencies_();

  var stage = job.stage || 'DISCOVERY';
  var query = job.title_query;
  var yearHint = job.release_year_hint || extractYearHintFromQuery_(query) || '';
  var dirtyRecon = false;
  var progressed = false;

  job.status = 'running';
  job.attempts = Number(job.attempts || 0) + 1;
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);

  try {
    if (stage === 'DISCOVERY') {
      progressed = lookupStageDiscovery_(ss, job, query, yearHint);
    } else if (stage === 'ELCINEMA_TITLE') {
      var s1 = lookupStageElCinemaTitle_(ss, job, query, yearHint);
      progressed = s1.progressed; dirtyRecon = dirtyRecon || s1.dirtyRecon;
    } else if (stage === 'ELCINEMA_RELEASED') {
      progressed = lookupStageElCinemaReleased_(ss, job);
    } else if (stage === 'BOM_TITLE') {
      var s2 = lookupStageBomTitle_(ss, job, query, yearHint);
      progressed = s2.progressed; dirtyRecon = dirtyRecon || s2.dirtyRecon;
    } else if (stage === 'BOM_RELEASE') {
      var s3 = lookupStageBomRelease_(ss, job, query, yearHint);
      progressed = s3.progressed; dirtyRecon = dirtyRecon || s3.dirtyRecon;
    } else if (stage === 'SUPPLEMENTAL_CHARTS') {
      var s4 = lookupStageSupplementalCharts_(ss, job, query, yearHint);
      progressed = s4.progressed; dirtyRecon = dirtyRecon || s4.dirtyRecon;
    } else if (stage === 'FINALIZE') {
      finalizeLookupJob_(ss, job);
      progressed = true; dirtyRecon = true;
    }
  } catch (e) {
    appendJobSourceStatus_(ss, job, {
      source: 'JOB_STAGE:' + stage,
      status: 'ERROR',
      rows: 0,
      notes: e.message
    });
    appendJobAnomaly_(ss, job, 'Stage ' + stage + ' failed: ' + e.message);
    job.last_error = e.message;
    job.status = 'failed';
    job.updated_at = isoNow_();
    upsertLookupJob_(ss, job);
    enqueueReview_(ss, {
      review_type: 'parser_anomaly',
      status: 'open',
      severity: 'high',
      film_title_raw: query,
      film_id_candidate: job.film_id || '',
      country: '',
      period_key: '',
      source_name: 'LOOKUP_QUEUE',
      source_url: '',
      details_json: JSON.stringify({ stage: stage, error: e.message, job_id: job.job_id }),
      analyst_notes: ''
    });
    // Graceful failover: if raw evidence exists for the job, allow the UI to render it.
    dirtyRecon = true;
  }

  return { progressed: progressed, dirtyRecon: dirtyRecon };
}

function processLookupJobBudgetById_(ss, jobId, budgetMs) {
  var start = Date.now();
  var dirtyRecon = false;
  while ((Date.now() - start) < budgetMs) {
    var job = getLookupJobById_(ss, jobId);
    if (!job) break;
    if (job.status === 'complete' || job.status === 'failed' || job.stage === 'COMPLETE') break;
    var step = processSingleLookupJobStage_(ss, job);
    dirtyRecon = dirtyRecon || step.dirtyRecon;
    if (!step.progressed) break;
    if ((Date.now() - start) > (budgetMs - 2500)) break;
  }

  if (dirtyRecon) {
    rebuildReconciledEvidence_(ss);
  }

  var finalJob = getLookupJobById_(ss, jobId);
  if (finalJob && finalJob.active_lookup === 'yes') {
    var sheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
    var currentQuery = String(sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '').trim();
    if (normalizeLatinTitle_(currentQuery) === normalizeLatinTitle_(finalJob.title_query || '')) {
      renderLookup_(currentQuery, false);
      writeLookupJobBanner_(ss, finalJob, (finalJob.status || 'queued').toUpperCase());
      writeLookupJobFailureNotice_(ss, finalJob);
    }
  }
}

function persistRawRecordsForJob_(ss, job, rows) {
  rows = Array.isArray(rows) ? rows : [];
  if (!rows.length) return { added: 0, skipped: 0 };
  var normalized = rows.map(function(r) {
    var c = {};
    Object.keys(r || {}).forEach(function(k) { c[k] = r[k]; });
    c.run_id = job.job_id;
    return normalizeRawRecord_(c, job.job_id);
  });
  return appendRawEvidence_(ss, normalized, job.job_id);
}

function autopromoteFilmFromRows_(ss, job, rows) {
  if (!rows || !rows.length) return;
  var normalizedRows = rows.map(function(r) {
    var c = {};
    Object.keys(r || {}).forEach(function(k) { c[k] = r[k]; });
    c.run_id = job.job_id;
    return normalizeRawRecord_(c, job.job_id);
  });

  var best = normalizedRows[0];
  normalizedRows.forEach(function(r) {
    if (Number(r.match_confidence || 0) > Number(best.match_confidence || 0)) best = r;
  });

  var ctx = createRunContext_();
  var filmId = '';
  normalizedRows.some(function(r) {
    if (r.record_scope !== 'title') return false;
    filmId = resolveFilmIdForRaw_(ss, r, ctx);
    return !!filmId;
  });
  if (!filmId) return;
  var film = findFilmById_(ss, filmId);
  if (!film) return;
  job.film_id = filmId;
  job.matched_title = film.canonical_title || best.film_title_raw || '';
  job.matched_title_ar = film.canonical_title_ar || best.film_title_ar_raw || '';
  job.matched_release_year = film.release_year || best.release_year_hint || '';
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
}

function writeLookupJobBanner_(ss, job, statusLabel) {
  var sheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  if (!sheet) return;
  var bannerRange = sheet.getRange('G1:J6');
  bannerRange.clearContent().clearFormat();
  sheet.getRange('G1').setValue('Lookup Queue Status').setFontWeight('bold').setBackground('#E5E7EB');

  if (!job) {
    sheet.getRange('G2:H5').setValues([
      ['Status', statusLabel || 'Idle'],
      ['Stage', ''],
      ['Job ID', ''],
      ['Last Update', '']
    ]);
    return;
  }

  var effectiveStatus = String(job.status || statusLabel || 'queued').toUpperCase();
  var stage = String(job.stage || '');
  var lastUpdate = String(job.updated_at || job.created_at || '');
  var filmId = String(job.film_id || '');
  var matched = String(job.matched_title || '');
  var year = String(job.matched_release_year || '');
  var error = String(job.last_error || '');

  sheet.getRange('G2:H6').setValues([
    ['Status', effectiveStatus],
    ['Stage', stage],
    ['Job ID', job.job_id || ''],
    ['Last Update', lastUpdate],
    ['Errors', error]
  ]);
  sheet.getRange('I2:J4').setValues([
    ['Matched Film', matched],
    ['Film ID', filmId],
    ['Release Year', year]
  ]);

  var statusCell = sheet.getRange('H2');
  if (effectiveStatus === 'FAILED') statusCell.setBackground('#FEE2E2').setFontColor('#991B1B').setFontWeight('bold');
  else if (effectiveStatus === 'COMPLETE') statusCell.setBackground('#DCFCE7').setFontColor('#166534').setFontWeight('bold');
  else if (effectiveStatus === 'RUNNING') statusCell.setBackground('#DBEAFE').setFontColor('#1D4ED8').setFontWeight('bold');
  else statusCell.setBackground('#FEF3C7').setFontColor('#92400E').setFontWeight('bold');
}

function writeLookupJobFailureNotice_(ss, job) {
  var sheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  if (!sheet || !job) return;
  var startRow = 12;
  if (String(job.status || '').toLowerCase() !== 'failed') {
    sheet.getRange(startRow, 1, 2, 10).clearContent();
    return;
  }
  var sourceStatus = parseJsonArray_(job.source_status_json);
  var successfulFetches = sourceStatus.filter(function(s) {
    return String(s.status || '').toUpperCase() === 'OK' && Number(s.rows || 0) > 0;
  });
  sheet.getRange(startRow, 1).setValue('Pipeline Status Notice').setFontWeight('bold').setBackground('#FDE68A');
  var msg = successfulFetches.length
    ? 'Live retrieval found evidence, but a downstream processing stage failed. Review Lookup_Jobs and Review_Queue before trusting the summary.'
    : 'Lookup job failed before any usable evidence could be normalized. Review Lookup_Jobs and Review_Queue.';
  sheet.getRange(startRow + 1, 1, 1, 10).merge().setValue(msg).setWrap(true).setBackground('#FFF7ED');
}

function normalizeIsoDateTime_(v) {
  if (!v) return '';
  if (Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v.getTime())) return v.toISOString();
  var s = String(v).trim();
  if (!s) return '';
  var d = new Date(s);
  if (!isNaN(d.getTime())) return d.toISOString();
  return s;
}

function normalizeIsoDate_(v) {
  if (!v) return '';
  if (Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v.getTime())) return v.toISOString().slice(0, 10);
  var s = String(v).trim();
  if (!s) return '';
  var d = new Date(s);
  if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  var m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : s;
}

function coerceNumberOrBlank_(v, blankFallback) {
  if (v === '' || v === null || typeof v === 'undefined') return blankFallback;
  if (typeof v === 'number') return isNaN(v) ? blankFallback : v;
  var s = String(v).replace(/,/g, '').trim();
  if (!s) return blankFallback;
  var n = Number(s);
  return isNaN(n) ? blankFallback : n;
}

function normalizeMarketCode_(code) {
  code = String(code || '').trim().toUpperCase();
  return CONFIG.MARKETS[code] ? code : code;
}

function normalizeYearHint_(yearHint) {
  if (yearHint === '' || yearHint === null || typeof yearHint === 'undefined') return '';
  var m = String(yearHint).match(/(19|20)\d{2}/);
  return m ? m[0] : String(yearHint);
}

function derivePeriodKeySafe_(r) {
  if (r.period_key) return String(r.period_key);
  if (r.record_granularity === 'week' && r.period_label_raw) return String(r.period_label_raw);
  if (r.record_granularity === 'weekend' && r.period_label_raw) return String(r.period_label_raw);
  if (r.record_granularity === 'year' && r.release_year_hint) return String(r.release_year_hint);
  if (r.period_start_date) return String(r.period_start_date);
  return '';
}

function stringifyJsonSafe_(v) {
  if (typeof v === 'string') {
    try { JSON.parse(v); return v; } catch (e) { return JSON.stringify({ value: v }); }
  }
  try { return JSON.stringify(v); } catch (e2) { return '{}'; }
}

function makeStableRawKey_(r) {
  var parts = [
    r.source_name || '',
    r.source_entity_id || '',
    r.country_code || '',
    normalizeLatinTitle_(r.film_title_raw || ''),
    normalizeArabicTitle_(r.film_title_ar_raw || ''),
    r.release_year_hint || '',
    r.record_scope || '',
    r.record_granularity || '',
    r.record_semantics || '',
    r.period_key || '',
    r.rank === '' ? '' : String(r.rank)
  ];
  var raw = parts.join('|');
  var digest = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, raw, Utilities.Charset.UTF_8);
  return digest.map(function(b) {
    var v = (b < 0 ? b + 256 : b).toString(16);
    return v.length === 1 ? '0' + v : v;
  }).join('');
}



// ============================================================
// SECTION 21: V5.2 MARKET COVERAGE UPGRADE
// Purpose:
//   - improve UAE / Kuwait support using Box Office Mojo title markets and The Numbers country pages
//   - fix weekly sorting and latest-period logic
//   - improve lookup clarity and source-status messaging
// ============================================================

function lookupPriorityMarkets_() {
  return ['AE', 'SA', 'EG', 'KW', 'QA', 'BH', 'OM'];
}

function periodSortValue_(r) {
  r = r || {};
  var key = String(r.period_key || r.period_label_raw || '').trim();
  var mRange = key.match(/(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})/);
  if (mRange) return parseInt(mRange[2].replace(/-/g, ''), 10);
  var mWeek = key.match(/(\d{4})-W(\d{1,2})/i) || key.match(/(\d{4})W(\d{1,2})/i);
  if (mWeek) return parseInt(mWeek[1], 10) * 100 + parseInt(mWeek[2], 10);
  var mMonth = key.match(/(\d{4})-(\d{2})$/);
  if (mMonth) return parseInt(mMonth[1], 10) * 100 + parseInt(mMonth[2], 10);
  var mYear = key.match(/^(19|20)\d{2}$/);
  if (mYear) return parseInt(key, 10) * 100;
  var date = String(r.period_end_date || r.period_start_date || '').slice(0, 10);
  if (date && /^\d{4}-\d{2}-\d{2}$/.test(date)) return parseInt(date.replace(/-/g, ''), 10);
  if (String(r.record_granularity || '').toLowerCase() === 'lifetime') return 99999999;
  return -1;
}

function sourceEvidencePriority_(r) {
  var sem = String((r && r.record_semantics) || '');
  if (sem === 'title_period_gross') return 4;
  if (sem === 'title_cumulative_total') return 3;
  if (sem === 'market_chart_topline') return 2;
  return 1;
}

function sortReconRowsForLookup_(a, b) {
  var countryCmp = String(a.country_code || '').localeCompare(String(b.country_code || ''));
  if (countryCmp !== 0) return countryCmp;
  var priCmp = sourceEvidencePriority_(b) - sourceEvidencePriority_(a);
  if (priCmp !== 0) return priCmp;
  var periodCmp = periodSortValue_(b) - periodSortValue_(a);
  if (periodCmp !== 0) return periodCmp;
  var dateCmp = String(b.period_start_date || b.period_end_date || '').localeCompare(String(a.period_start_date || a.period_end_date || ''));
  if (dateCmp !== 0) return dateCmp;
  return String(a.source_name || '').localeCompare(String(b.source_name || ''));
}

function latestComparableRecordForMarket_(list) {
  list = (list || []).slice().filter(Boolean);
  if (!list.length) return null;
  list.sort(function(a, b) {
    var pa = sourceEvidencePriority_(a), pb = sourceEvidencePriority_(b);
    if (pb !== pa) return pb - pa;
    var va = periodSortValue_(a), vb = periodSortValue_(b);
    if (vb !== va) return vb - va;
    var da = String(a.period_end_date || a.period_start_date || '');
    var db = String(b.period_end_date || b.period_start_date || '');
    return db.localeCompare(da);
  });
  return list[0] || null;
}

function normalizeSourceTypeLabel_(row) {
  var sem = String(row.record_semantics || '');
  if (sem === 'title_period_gross') return 'title_weekly';
  if (sem === 'title_cumulative_total') return 'title_market_total';
  if (sem === 'market_chart_topline') return 'chart_signal';
  return sem || 'other';
}

function inferFreshnessModeLabel_(job) {
  if (!job) return [{ source: 'NO_JOB', status: 'No lookup job found', rows: '', notes: 'No matching queued or completed lookup job for this title.' }];
  var statusList = parseJsonArray_(job.source_status_json);
  var successful = statusList.filter(function(s) { return String(s.status || '').toUpperCase() === 'OK'; });
  if (!successful.length) {
    return [{ source: 'CACHE_ONLY', status: 'No successful live source hits recorded', rows: '', notes: 'Report rendered from existing local reconciled data.' }];
  }
  return [{
    source: 'LIVE_RUN_COMPLETED',
    status: String(job.status || '').toUpperCase(),
    rows: successful.reduce(function(sum, s) { return sum + Number(s.rows || 0); }, 0),
    notes: 'Report rendered from cached live retrieval dataset. Last live retrieval: ' + (job.updated_at || '')
  }].concat(successful.map(function(s) {
    return {
      source: s.source,
      status: s.status,
      rows: s.rows,
      notes: s.notes
    };
  }));
}

function getMostRelevantLookupJob_(ss, query) {
  var jobs = readSheetObjects_(ensureLookupJobsSheet_(ss));
  var nq = normalizeLatinTitle_(query);
  var candidates = jobs.filter(function(j) {
    return normalizeLatinTitle_(j.title_query || '') === nq;
  });
  candidates.sort(function(a, b) {
    return String(b.updated_at || '').localeCompare(String(a.updated_at || ''));
  });
  return candidates[0] || null;
}

function getLatestLookupJobByQuery_(ss, query) {
  var jobs = readSheetObjects_(ensureLookupJobsSheet_(ss));
  var nq = normalizeLatinTitle_(query);
  var candidates = jobs.filter(function(j) {
    return normalizeLatinTitle_(j.title_query || '') === nq;
  });

  candidates.sort(function(a, b) {
    var aActive = String(a.active_lookup || '').toLowerCase() === 'yes' ? 1 : 0;
    var bActive = String(b.active_lookup || '').toLowerCase() === 'yes' ? 1 : 0;
    if (bActive !== aActive) return bActive - aActive;

    var aUpdated = String(a.updated_at || '');
    var bUpdated = String(b.updated_at || '');
    return bUpdated.localeCompare(aUpdated);
  });

  return candidates[0] || null;
}

function findTheNumbersCountrySlug_(countryCode) {
  var map = {
    AE: 'United-Arab-Emirates',
    KW: 'Kuwait',
    SA: 'Saudi-Arabia',
    EG: 'Egypt',
    QA: 'Qatar',
    BH: 'Bahrain',
    OM: 'Oman',
    JO: 'Jordan',
    LB: 'Lebanon'
  };
  return map[countryCode] || '';
}

function normalizeTheNumbersCountryUrl_(url) {
  url = cleanupSearchResultUrl_(url);
  if (!url) return '';
  var m = url.match(/^https?:\/\/www\.the-numbers\.com\/movie\/[^?#]+\/(United-Arab-Emirates|Kuwait|Saudi-Arabia|Egypt|Qatar|Bahrain|Oman|Jordan|Lebanon)\/?$/i);
  return m ? url.replace(/\/+$/, '') : '';
}

function discoverTheNumbersCountryCandidates_(query, yearHint, targetMarketCodes) {
  var byUrl = {};
  var codes = unique_((targetMarketCodes || []).filter(Boolean));
  if (!codes.length) codes = ['AE', 'KW', 'SA'];
  var searchQueries = [];
  codes.forEach(function(code) {
    var countrySlug = findTheNumbersCountrySlug_(code);
    var countryName = (CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || countrySlug.replace(/-/g, ' ');
    if (!countrySlug) return;
    searchQueries.push('site:the-numbers.com/movie/ "' + query + '" "' + countryName + '"');
    if (yearHint) searchQueries.push('site:the-numbers.com/movie/ "' + query + '" "' + countryName + '" "' + yearHint + '"');
  });
  unique_(searchQueries).forEach(function(sq) {
    searchEngineCandidates_(sq).forEach(function(hit) {
      var url = normalizeTheNumbersCountryUrl_(hit.url);
      if (!url) return;
      var score = scoreCandidateHit_(query, (hit.title || '') + ' ' + (hit.snippet || ''), yearHint);
      if (!byUrl[url] || score > byUrl[url].score) byUrl[url] = { url: url, score: score };
    });
  });
  return Object.keys(byUrl).sort(function(a, b) { return byUrl[b].score - byUrl[a].score; }).map(function(url) { return byUrl[url]; }).slice(0, 6);
}

function extractNumbersLikeDollarValues_(text, maxCount) {
  var vals = [];
  var m, rx = /\$([\d,]+)/g;
  while ((m = rx.exec(text)) !== null) {
    vals.push(parseMoney_(m[1]));
    if (maxCount && vals.length >= maxCount) break;
  }
  return vals.filter(function(v) { return Number(v) > 0; });
}

function fetchTheNumbersCountryEvidenceByUrl_(url, query, yearHint) {
  url = normalizeTheNumbersCountryUrl_(url);
  if (!url) throw new Error('Invalid The Numbers country URL');
  var html = fetchUrl_(url);
  var text = htmlToText_(html);

  var urlCountrySlug = (url.match(/\/(United-Arab-Emirates|Kuwait|Saudi-Arabia|Egypt|Qatar|Bahrain|Oman|Jordan|Lebanon)$/i) || [])[1] || '';
  var country = urlCountrySlug ? urlCountrySlug.replace(/-/g, ' ') : '';
  var countryCode = findMarketCodeByCountry_(country);
  if (!countryCode) return [];

  var og = html.match(/<meta[^>]+property="og:title"[^>]+content="([^"]+)"/i);
  var pageTitle = og ? decodeHtmlEntitiesSimple_(og[1] || '') : ((html.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [])[1] || '');
  pageTitle = normalizeWhitespace_(htmlToText_(pageTitle));
  var title = pageTitle.replace(/\s*-\s*The Numbers.*$/i, '').replace(/\s*\/\s*' + escapeRegex_(country) + '.*$/i, '').trim();
  var year = '';
  var mYear = pageTitle.match(/\((20\d{2})\)/);
  if (mYear) year = mYear[1];
  if (!titleMatchesQuery_(query, title, '', year, yearHint)) {
    var score = scoreResolvedTitle_(query, title, '', year, yearHint);
    if (score < 0.68) return [];
  }

  var releaseDate = '';
  var releaseM = text.match(/Release Date\s+([A-Za-z]{3}\s+\d{1,2},\s+20\d{2})/i) || text.match(/Released\s+([A-Za-z]{3}\s+\d{1,2},\s+20\d{2})/i);
  if (releaseM) releaseDate = toIsoDateFromLong_(releaseM[1]);

  var values = extractNumbersLikeDollarValues_(text, 12);
  if (!values.length) return [];
  var total = Math.max.apply(null, values);
  if (!total || total <= 0) return [];

  var notes = 'Country page total from The Numbers; summary-level market evidence only';
  return [buildRawRecord_({
    source_name: 'THENUMBERS_COUNTRY_PAGE',
    source_url: url,
    parser_confidence: 0.72,
    source_entity_id: url,
    country: country,
    country_code: countryCode,
    film_title_raw: title,
    film_title_ar_raw: '',
    release_year_hint: year || yearHint || '',
    record_scope: 'title',
    record_granularity: 'lifetime',
    record_semantics: 'title_cumulative_total',
    source_confidence: 0.62,
    match_confidence: 0.84,
    evidence_type: 'title_market_total',
    period_label_raw: 'lifetime',
    period_start_date: releaseDate || '',
    period_end_date: '',
    period_key: '',
    rank: '',
    period_gross_local: '',
    cumulative_gross_local: total,
    currency: 'USD',
    admissions_actual: '',
    work_id: '',
    distributor: '',
    notes: notes,
    raw_payload_json: JSON.stringify({ values: values.slice(0, 12), country: country, title: title, release_date: releaseDate })
  })];
}

function parseBoxOfficeMojoTitleAreaRows_(html, text, title, year) {
  var rows = [];
  var seen = {};
  Object.keys(CONFIG.MARKETS).forEach(function(code) {
    var country = CONFIG.MARKETS[code].country;
    var countryPattern = escapeRegex_(country);
    var re = new RegExp(countryPattern + '[\\s\\S]{0,220}?([A-Z][a-z]{2}\\.?\\s+\\d{1,2},\\s+20\\d{2})?[\\s\\S]{0,160}?\\$([\\d,]+)(?:[\\s\\S]{0,120}?\\$([\\d,]+))?', 'ig');
    var m;
    while ((m = re.exec(html)) !== null) {
      var releaseDate = toIsoDateFromLong_((m[1] || '').replace(/\./g, ''));
      var firstDollar = parseMoney_(m[2]);
      var secondDollar = parseMoney_(m[3]);
      var opening = '';
      var gross = '';
      if (secondDollar && secondDollar >= firstDollar) {
        opening = firstDollar;
        gross = secondDollar;
      } else {
        gross = firstDollar;
      }
      if (!gross) continue;
      var key = [code, releaseDate, opening || '', gross].join('|');
      if (seen[key]) continue;
      seen[key] = true;
      rows.push(buildRawRecord_({
        source_name: 'BOXOFFICEMOJO_TITLE_MARKETS',
        source_url: '',
        parser_confidence: secondDollar ? 0.88 : 0.74,
        source_entity_id: 'title_area|' + key,
        country: country,
        country_code: code,
        film_title_raw: title || '',
        film_title_ar_raw: '',
        release_year_hint: year || '',
        record_scope: 'title',
        record_granularity: 'lifetime',
        record_semantics: 'title_cumulative_total',
        source_confidence: 0.70,
        match_confidence: 0.90,
        evidence_type: 'title_market_total',
        period_label_raw: 'lifetime',
        period_start_date: releaseDate || '',
        period_end_date: '',
        period_key: '',
        rank: '',
        period_gross_local: opening || '',
        cumulative_gross_local: gross,
        currency: 'USD',
        admissions_actual: '',
        work_id: '',
        distributor: '',
        notes: opening ? ('Box Office Mojo title-page market row; opening=' + opening + ' USD') : 'Box Office Mojo title-page market row',
        raw_payload_json: JSON.stringify({ country: country, release_date: releaseDate, opening: opening, gross: gross })
      }));
    }
  });
  return dedupRawRecords_(rows);
}

function buildBoxOfficeMojoTitleSummaryRows_(titleInfo) {
  if (!titleInfo) return [];
  var rows = [];
  if (titleInfo.area_rows && titleInfo.area_rows.length) rows = rows.concat(titleInfo.area_rows);
  if (titleInfo.title_en && titleInfo.intl_gross_usd) {
    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_TITLE',
      source_url: titleInfo.source_url,
      parser_confidence: 0.80,
      source_entity_id: titleInfo.title_url || titleInfo.source_url,
      country: 'All Territories',
      country_code: '',
      film_title_raw: titleInfo.title_en,
      film_title_ar_raw: '',
      release_year_hint: titleInfo.release_year || '',
      record_scope: 'global',
      record_granularity: 'lifetime',
      record_semantics: 'title_cumulative_total',
      source_confidence: 0.58,
      match_confidence: 0.88,
      evidence_type: 'title_market_total',
      period_label_raw: 'lifetime',
      period_start_date: '',
      period_end_date: '',
      period_key: '',
      rank: '',
      period_gross_local: '',
      cumulative_gross_local: titleInfo.intl_gross_usd,
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor: '',
      notes: 'International total from Box Office Mojo title page; global evidence only',
      raw_payload_json: JSON.stringify(titleInfo)
    }));
  }
  return dedupRawRecords_(rows);
}

function lookupStageBomRelease_(ss, job, query, yearHint) {
  var discovered = parseJsonArray_(job.discovered_release_urls_json);
  var processed = parseJsonArray_(job.processed_release_urls_json);
  var next = discovered.filter(function(url) { return processed.indexOf(url) < 0; })[0];
  if (!next) {
    job.stage = 'SUPPLEMENTAL_CHARTS';
    job.updated_at = isoNow_();
    upsertLookupJob_(ss, job);
    return { progressed: true, dirtyRecon: false };
  }

  var relRows = [];
  try {
    relRows = (fetchBoxOfficeMojoReleaseEvidence_(next, query, yearHint) || []).filter(function(r) {
      return titleMatchesQuery_(query, r.film_title_raw, r.film_title_ar_raw || '', r.release_year_hint, yearHint);
    });
    appendJobSourceStatus_(ss, job, {
      source: 'BOXOFFICEMOJO_RELEASE',
      status: 'OK',
      rows: relRows.length,
      notes: relRows.length ? 'Fetched territory-specific release evidence' : 'Release page resolved but yielded no usable rows'
    });
    if (!relRows.length) appendJobAnomaly_(ss, job, 'Box Office Mojo release page resolved with zero usable rows: ' + next);
  } catch (e) {
    appendJobSourceStatus_(ss, job, { source: 'BOXOFFICEMOJO_RELEASE', status: 'ERROR', rows: 0, notes: e.message });
    appendJobAnomaly_(ss, job, 'Box Office Mojo release page failed for ' + next + ': ' + e.message);
  }

  var targetMarkets = lookupPriorityMarkets_().filter(function(code) {
    return ['AE', 'KW', 'SA'].indexOf(code) >= 0;
  });
  var numbersRows = [];
  try {
    var nums = discoverTheNumbersCountryCandidates_(job.matched_title || query, yearHint || job.matched_release_year || '', targetMarkets);
    nums.forEach(function(hit) {
      var hitRows = fetchTheNumbersCountryEvidenceByUrl_(hit.url, job.matched_title || query, yearHint || job.matched_release_year || '');
      if (hitRows && hitRows.length) numbersRows = numbersRows.concat(hitRows);
    });
    appendJobSourceStatus_(ss, job, {
      source: 'THENUMBERS_COUNTRY_PAGE',
      status: 'OK',
      rows: numbersRows.length,
      notes: numbersRows.length ? 'Fetched The Numbers country-page totals' : 'No usable The Numbers country-page totals found'
    });
  } catch (e2) {
    appendJobSourceStatus_(ss, job, { source: 'THENUMBERS_COUNTRY_PAGE', status: 'ERROR', rows: 0, notes: e2.message });
    appendJobAnomaly_(ss, job, 'The Numbers country-page fetch failed: ' + e2.message);
  }

  var allRows = dedupRawRecords_(relRows.concat(numbersRows));
  if (allRows.length) {
    persistRawRecordsForJob_(ss, job, allRows);
    autopromoteFilmFromRows_(ss, job, allRows);
    job.dirty_recon = 'yes';
  }

  processed.push(next);
  job.processed_release_urls_json = JSON.stringify(unique_(processed));
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
  return { progressed: true, dirtyRecon: allRows.length > 0 };
}

function buildAcquisitionWarnings_(query, rows, statuses, releaseMarkets, match, anomalies) {
  var warnings = [];
  var titleRows = rows.filter(function(r) { return r.record_scope === 'title'; });
  var weeklyMarkets = unique_(titleRows.filter(function(r) { return r.record_semantics === 'title_period_gross'; }).map(function(r) { return r.country_code; }).filter(Boolean));
  var perfMarkets = unique_(titleRows.map(function(r) { return r.country_code; }).filter(Boolean));
  var releaseCodes = unique_((releaseMarkets || []).map(function(x) { return x.country_code || x; }).filter(Boolean));
  var releaseOnly = releaseCodes.filter(function(code) { return perfMarkets.indexOf(code) < 0; });
  var priorityMissing = lookupPriorityMarkets_().filter(function(code) { return perfMarkets.indexOf(code) < 0; });
  var sources = unique_(titleRows.map(function(r) { return r.source_name; }));
  var failed = (statuses || []).filter(function(s) { return String(s.status || '').toUpperCase() === 'ERROR'; });

  if (!rows.length) warnings.push('No local or live evidence found for this query. This should be treated as retrieval failure, not proof of no market performance.');
  if (!match || !match.film_id) warnings.push('Canonical identity is not fully locked. Live resolution may be usable, but the workbook master identity still needs confirmation.');
  if (weeklyMarkets.length < 3) warnings.push('Weekly title-performance evidence exists in fewer than 3 target markets. Cross-market comparability remains weak.');
  if (sources.length <= 1 && titleRows.length) warnings.push('Title-level evidence is concentrated in a single source. Treat the read as provisional until another source confirms key markets.');
  if (releaseOnly.length) warnings.push('Released markets exceed performance markets: ' + releaseOnly.join(', ') + ' are released but currently lack title-performance evidence in this workbook.');
  if (priorityMissing.length) warnings.push('Missing priority markets still unconfirmed: ' + priorityMissing.join(', ') + '.');
  if (rows.some(function(r) { return r.approx_ticket_estimate && ['low', 'medium'].indexOf(String(r.ticket_estimate_confidence || '').toLowerCase()) >= 0; })) {
    warnings.push('Ticket estimates are approximate and should not be treated as decision-grade demand KPIs.');
  }
  if (failed.length) warnings.push('One or more live source fetches failed: ' + failed.map(function(x) { return x.source; }).join(', '));
  (anomalies || []).slice(0, 4).forEach(function(a) { warnings.push(a); });
  return unique_(warnings);
}

function buildAcquisitionSummary_(rows, releaseMarkets, match, statuses) {
  var titleRows = rows.filter(function(r) { return r.record_scope === 'title'; });
  var weeklyRows = titleRows.filter(function(r) { return r.record_semantics === 'title_period_gross'; });
  var cumulativeRows = titleRows.filter(function(r) { return r.record_semantics === 'title_cumulative_total'; });
  var weeklyMarkets = unique_(weeklyRows.map(function(r) { return r.country_code; }).filter(Boolean));
  var perfMarkets = unique_(titleRows.map(function(r) { return r.country_code; }).filter(Boolean));
  var releaseCodes = unique_((releaseMarkets || []).map(function(x) { return x.country_code || x; }).filter(Boolean));
  var missing = lookupPriorityMarkets_().filter(function(code) { return perfMarkets.indexOf(code) < 0; });
  var best = rankBestMarkets_(weeklyRows, cumulativeRows).slice(0, 3);
  var sourceCounts = {};
  titleRows.forEach(function(r) { sourceCounts[r.source_name] = (sourceCounts[r.source_name] || 0) + 1; });
  var topSource = Object.keys(sourceCounts).sort(function(a, b) { return sourceCounts[b] - sourceCounts[a]; })[0] || '';
  var totalSources = Object.keys(sourceCounts).length;

  var strength = 'Weak';
  if (weeklyMarkets.length >= 3 && totalSources >= 2) strength = 'Strong';
  else if (weeklyMarkets.length >= 2 || (weeklyMarkets.length >= 1 && perfMarkets.length >= 3)) strength = 'Moderate';
  else if (titleRows.length) strength = 'Limited';

  var take = 'Insufficient evidence for a confident acquisition read.';
  if (weeklyRows.length) {
    var byCountry = {};
    weeklyRows.forEach(function(r) {
      if (!byCountry[r.country_code]) byCountry[r.country_code] = [];
      byCountry[r.country_code].push(r);
    });
    var rankedCodes = Object.keys(byCountry).sort(function(a, b) { return byCountry[b].length - byCountry[a].length; });
    var parts = [];
    rankedCodes.slice(0, 2).forEach(function(code) {
      var list = byCountry[code].slice().sort(function(a, b) { return periodSortValue_(a) - periodSortValue_(b); });
      var peak = 0;
      list.forEach(function(r) { peak = Math.max(peak, Number(r.period_gross_local || 0)); });
      parts.push((CONFIG.MARKETS[code] ? CONFIG.MARKETS[code].country : code) + ' shows ' + list.length + ' tracked weeks and a peak weekly gross of ~' + formatNumberShort_(peak) + ' ' + (list[0] ? (list[0].currency || '') : ''));
    });
    var cumulativeMarkets = unique_(cumulativeRows.filter(function(r) { return weeklyMarkets.indexOf(r.country_code) < 0 && r.country_code; }).map(function(r) { return r.country_code; }));
    if (cumulativeMarkets.length) {
      parts.push('Additional summary-level market totals exist for ' + cumulativeMarkets.map(function(code) { return CONFIG.MARKETS[code] ? CONFIG.MARKETS[code].country : code; }).join(', ') + '.');
    }
    take = parts.join('. ') + '.';
  } else if (cumulativeRows.length) {
    take = 'Only title-summary market totals are available. This is helpful for sizing territories but weak for run-quality analysis.';
  }

  return {
    matched_film: (match && match.title) || (titleRows[0] && (titleRows[0].canonical_title || titleRows[0].film_title_raw)) || '',
    match_confidence: (match && match.score) || '',
    identity_status: (match && match.film_id) ? 'matched' : 'unmatched',
    release_year: (match && match.release_year) || (titleRows[0] && (titleRows[0].release_year || titleRows[0].release_year_hint)) || '',
    evidence_strength: strength,
    best_markets: best.join(', ') || 'None',
    missing_priority_markets: missing.join(', ') || 'None',
    weekly_markets: weeklyMarkets.join(', ') || 'None',
    source_concentration: totalSources ? (topSource + ' (' + sourceCounts[topSource] + ' rows, ' + totalSources + ' source' + (totalSources > 1 ? 's' : '') + ')') : 'None',
    analyst_take: take
  };
}

function buildMarketCoverageV2_(rows, releaseMarkets) {
  var byMarket = {};
  rows.forEach(function(r) {
    if (!r.country_code) return;
    if (!byMarket[r.country_code]) byMarket[r.country_code] = [];
    byMarket[r.country_code].push(r);
  });
  var releaseSet = {};
  (releaseMarkets || []).forEach(function(x) {
    var code = x.country_code || x;
    if (code) releaseSet[code] = true;
  });

  var out = [];
  Object.keys(CONFIG.MARKETS).forEach(function(code) {
    var list = byMarket[code] || [];
    var score = computeCoverageScore_(list, !!releaseSet[code]);
    if (!list.length) {
      out.push([CONFIG.MARKETS[code].country, score, scoreLabel_(score), '', '', '', 0, releaseSet[code] ? 'released_only' : '', '']);
      return;
    }
    var latest = latestComparableRecordForMarket_(list);
    var perf = list.filter(function(r) { return r.record_scope === 'title' && r.record_semantics === 'title_period_gross'; });
    var strongest = perf.length ? 'Title weekly performance' : (list.some(function(r) { return r.record_semantics === 'title_cumulative_total'; }) ? 'Title market total' : 'Chart mention only');
    out.push([
      (latest && latest.country) || CONFIG.MARKETS[code].country,
      score,
      strongest,
      (latest && (latest.period_key || latest.period_label_raw)) || '',
      (latest && latest.period_gross_local) || '',
      Math.max.apply(null, list.map(function(r) { return Number(r.cumulative_gross_local || 0); }).concat([0])) || '',
      perf.length || list.length,
      (latest && latest.freshness_status) || '',
      (latest && latest.source_name) || ''
    ]);
  });
  return out;
}

function buildComparablePerformanceRowsV2_(rows) {
  var filtered = rows.filter(function(r) {
    return r.record_scope === 'title' && (r.record_semantics === 'title_period_gross' || r.record_semantics === 'title_cumulative_total');
  });

  var weeklyKeys = {};
  filtered.forEach(function(r) {
    if (r.record_semantics === 'title_period_gross') {
      var k = [r.film_id || '', r.country_code || '', r.source_name || ''].join('|');
      weeklyKeys[k] = true;
    }
  });

  filtered = filtered.filter(function(r) {
    if (r.record_semantics !== 'title_cumulative_total') return true;
    var k = [r.film_id || '', r.country_code || '', r.source_name || ''].join('|');
    return !weeklyKeys[k];
  });

  filtered.sort(function(a, b) {
    var countryCmp = String(a.country_code || '').localeCompare(String(b.country_code || ''));
    if (countryCmp !== 0) return countryCmp;
    var pri = sourceEvidencePriority_(b) - sourceEvidencePriority_(a);
    if (pri !== 0) return pri;
    var periodCmp = periodSortValue_(a) - periodSortValue_(b);
    if (periodCmp !== 0) return periodCmp;
    var da = String(a.period_end_date || a.period_start_date || '');
    var db = String(b.period_end_date || b.period_start_date || '');
    return da.localeCompare(db);
  });

  return filtered.map(function(r) {
    return [
      r.country,
      r.record_granularity === 'lifetime' ? 'total' : r.record_granularity,
      r.period_key || r.period_label_raw || 'lifetime',
      r.period_gross_local || '',
      r.currency || '',
      r.cumulative_gross_local || '',
      r.approx_ticket_estimate || '',
      r.ticket_estimate_confidence || '',
      r.source_name,
      r.freshness_status,
      evidenceGrade_(r)
    ];
  });
}

function buildSourceCoverageMatrix_(rows, statuses) {
  var bySource = {};
  rows.forEach(function(r) {
    var key = r.source_name;
    if (!bySource[key]) bySource[key] = { markets: {}, rows: 0, types: {} };
    bySource[key].rows += 1;
    if (r.country_code) bySource[key].markets[r.country_code] = true;
    bySource[key].types[normalizeSourceTypeLabel_(r)] = true;
  });
  var out = Object.keys(bySource).sort().map(function(source) {
    var x = bySource[source];
    var type = Object.keys(x.types).join(', ');
    var markets = Object.keys(x.markets).join(', ');
    var notes = (statuses || []).filter(function(s) { return String(s.source || '').indexOf(source) === 0; }).map(function(s) { return s.status + ': ' + s.notes; }).slice(0, 2).join(' | ');
    return [source, type, markets, x.rows, notes];
  });
  if (!out.length && statuses && statuses.length) {
    out = statuses.map(function(s) { return [s.source, '', '', s.rows, s.status + ': ' + s.notes]; });
  }
  return out;
}

function renderLookup_(query, forceLive) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  var sheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  setupLookupSheet_(ss, query);
  clearLookupOutput_(sheet);
  sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).setValue(query);

  var recon = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RECON));
  var films = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.FILMS));
  var aliases = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.ALIASES));
  var match = matchQueryToFilm_(query, films, aliases);
  var relevant = [];
  var normQueryEn = normalizeLatinTitle_(query);
  var normQueryAr = normalizeArabicTitle_(query);

  if (match.film_id) {
    relevant = recon.filter(function(r) { return r.film_id === match.film_id; });
  } else {
    relevant = recon.filter(function(r) {
      var t1 = normalizeLatinTitle_(r.canonical_title || r.film_title_raw || '');
      var t2 = normalizeArabicTitle_(r.canonical_title_ar || r.film_title_ar_raw || '');
      return (t1 && (t1.indexOf(normQueryEn) >= 0 || normQueryEn.indexOf(t1) >= 0 || titleSimilarity_(normQueryEn, t1) >= 0.75)) ||
             (t2 && (t2.indexOf(normQueryAr) >= 0 || normQueryAr.indexOf(t2) >= 0 || titleSimilarity_(normQueryAr, t2) >= 0.80));
    });
  }

  var live = forceLive ? fetchLiveLookupSignals_(query, match) : { rows: [], status: [], releaseMarkets: [], promotedFilm: null, anomalies: [] };
  var promoted = live.promotedFilm || null;
  if (!match.film_id && promoted && promoted.film_id) {
    match = {
      film_id: promoted.film_id,
      title: promoted.canonical_title,
      release_year: promoted.release_year,
      score: promoted.identity_confidence || 0.96,
      identity_confidence: promoted.identity_confidence || 'live_promoted',
      canonical_title_ar: promoted.canonical_title_ar || ''
    };
  }

  var allRows = dedupReconLikeRows_(relevant.concat(live.rows || []));
  if (match.film_id) {
    allRows = allRows.map(function(r) {
      if (!r.film_id) r.film_id = match.film_id;
      if (!r.canonical_title) r.canonical_title = match.title || r.film_title_raw || '';
      if (!r.canonical_title_ar) r.canonical_title_ar = match.canonical_title_ar || r.film_title_ar_raw || '';
      if (!r.release_year) r.release_year = match.release_year || r.release_year_hint || '';
      return r;
    });
  }

  var releaseMarkets = unique_((live.releaseMarkets || []).map(function(x) { return x.country_code; }).filter(Boolean));
  var latestJob = getMostRelevantLookupJob_(ss, query);
  if (latestJob) writeLookupJobBanner_(ss, latestJob, (latestJob.status || 'queued').toUpperCase());

  var warnings = buildAcquisitionWarnings_(query, allRows, live.status || [], releaseMarkets, match, live.anomalies || []);
  var summary = buildAcquisitionSummary_(allRows, releaseMarkets, match, live.status || []);
  var coverage = buildMarketCoverageV2_(allRows, releaseMarkets);
  var perfRows = buildComparablePerformanceRowsV2_(allRows);
  var chartRows = buildChartRowsV2_(allRows);
  var sourceMatrix = buildSourceCoverageMatrix_(allRows, live.status || []);

  var liveRefreshLabel = forceLive ? ('Started ' + isoNow_()) : (latestJob && (latestJob.status === 'complete' || latestJob.status === 'running' || latestJob.status === 'failed') ? ('Cached live run: ' + (latestJob.updated_at || '')) : 'No');
  var startRow = 6;
  var overview = [
    ['Matched Film', summary.matched_film || match.title || query, 'Match Confidence', summary.match_confidence || (match.score || '')],
    ['Film ID', match.film_id || '', 'Identity Status', summary.identity_status || (match.film_id ? 'matched' : 'unmatched')],
    ['Release Year', summary.release_year || match.release_year || '', 'Live Refresh', liveRefreshLabel]
  ];
  sheet.getRange(startRow, 1, overview.length, 4).setValues(overview);
  styleBlockHeader_(sheet.getRange(startRow, 1, 1, 4), '#E5E7EB');

  var summaryRow = startRow + 4;
  sheet.getRange(summaryRow, 1).setValue('Acquisition Summary').setFontWeight('bold').setBackground('#C7D2FE');
  var summaryBody = [
    ['Evidence Strength', summary.evidence_strength],
    ['Best-Supported Markets', summary.best_markets || 'None'],
    ['Missing Priority Markets', summary.missing_priority_markets || 'None'],
    ['Weekly Evidence Markets', summary.weekly_markets || 'None'],
    ['Source Concentration', summary.source_concentration || 'Unknown'],
    ['Analyst Take', summary.analyst_take || 'Insufficient evidence for a confident acquisition read.']
  ];
  sheet.getRange(summaryRow + 1, 1, summaryBody.length, 2).setValues(summaryBody);
  styleLabelValueBlock_(sheet, summaryRow + 1, summaryBody.length, 2);

  var warnRow = summaryRow + summaryBody.length + 2;
  sheet.getRange(warnRow, 1).setValue('Acquisition Warnings').setFontWeight('bold').setBackground('#FDE68A');
  var warnValues = warnings.length ? warnings.map(function(w) { return [w]; }) : [['No critical warnings generated.']];
  sheet.getRange(warnRow + 1, 1, warnValues.length, 1).setValues(warnValues);
  if (warnings.length) sheet.getRange(warnRow + 1, 1, warnValues.length, 1).setFontColor('#7C2D12');

  var coverageRow = warnRow + warnValues.length + 3;
  sheet.getRange(coverageRow, 1).setValue('Market Coverage Summary').setFontWeight('bold').setBackground('#DBEAFE');
  var coverageHeader = [['Country', 'Coverage Score', 'Strongest Evidence', 'Latest Period', 'Latest Gross', 'Latest Total', 'Weeks/Records', 'Freshness', 'Primary Source']];
  sheet.getRange(coverageRow + 1, 1, 1, coverageHeader[0].length).setValues(coverageHeader).setFontWeight('bold');
  if (coverage.length) sheet.getRange(coverageRow + 2, 1, coverage.length, coverageHeader[0].length).setValues(coverage);

  var perfRow = coverageRow + 3 + Math.max(coverage.length, 1);
  sheet.getRange(perfRow, 1).setValue('Comparable Title Performance Evidence').setFontWeight('bold').setBackground('#D1FAE5');
  var perfHeader = [['Country', 'Granularity', 'Period', 'Period Gross (Local)', 'Currency', 'Cumulative (Local)', 'Approx Tickets', 'Ticket Confidence', 'Source', 'Freshness', 'Evidence Grade']];
  sheet.getRange(perfRow + 1, 1, 1, perfHeader[0].length).setValues(perfHeader).setFontWeight('bold');
  if (perfRows.length) sheet.getRange(perfRow + 2, 1, perfRows.length, perfHeader[0].length).setValues(perfRows);
  else sheet.getRange(perfRow + 2, 1).setValue('No title-level comparable evidence available.');

  var chartRow = perfRow + 4 + Math.max(perfRows.length, 1);
  sheet.getRange(chartRow, 1).setValue('Chart Mentions / Market-Level Signals (Do Not Treat as Title Performance)').setFontWeight('bold').setBackground('#FECACA');
  var chartHeader = [['Country', 'Period', 'Top Title Mentioned', 'Metric', 'Currency', 'Source', 'Freshness']];
  sheet.getRange(chartRow + 1, 1, 1, chartHeader[0].length).setValues(chartHeader).setFontWeight('bold');
  if (chartRows.length) sheet.getRange(chartRow + 2, 1, chartRows.length, chartHeader[0].length).setValues(chartRows);
  else sheet.getRange(chartRow + 2, 1).setValue('No chart-mention rows found.');

  var sourceRow = chartRow + 4 + Math.max(chartRows.length, 1);
  sheet.getRange(sourceRow, 1).setValue('Source Coverage Matrix').setFontWeight('bold').setBackground('#F3E8FF');
  var sourceHeader = [['Source', 'Type', 'Markets', 'Rows', 'Status / Notes']];
  sheet.getRange(sourceRow + 1, 1, 1, sourceHeader[0].length).setValues(sourceHeader).setFontWeight('bold');
  if (sourceMatrix.length) sheet.getRange(sourceRow + 2, 1, sourceMatrix.length, sourceHeader[0].length).setValues(sourceMatrix);

  var statusRow = sourceRow + 4 + Math.max(sourceMatrix.length, 1);
  sheet.getRange(statusRow, 1).setValue('Live Source Status').setFontWeight('bold').setBackground('#E9D5FF');
  var statusHeader = [['Source', 'Status', 'Rows', 'Notes']];
  sheet.getRange(statusRow + 1, 1, 1, 4).setValues(statusHeader).setFontWeight('bold');
  var statuses = (live.status && live.status.length) ? live.status : inferFreshnessModeLabel_(latestJob);
  var statusValues = statuses.map(function(s) { return [s.source, s.status, s.rows, s.notes]; });
  sheet.getRange(statusRow + 2, 1, statusValues.length, 4).setValues(statusValues);

  autoResize_(sheet, 14);
}


// ============================================================
// V5.3 SOURCE EXECUTION PATCH
// - make BOM market-total extraction explicit and visible
// - add BOM yearly-market fallback parsing (AE/KW/SA priority)
// - improve The Numbers attempt visibility
// - keep source semantics strict: totals do not overwrite weekly evidence
// ============================================================

function normalizeBoxOfficeMojoYearMarketUrlV53_(year, countryCode) {
  year = String(year || '').trim();
  countryCode = String(countryCode || '').trim().toUpperCase();
  if (!year || !countryCode) return '';
  return 'https://www.boxofficemojo.com/year/' + year + '/?area=' + encodeURIComponent(countryCode) + '&grossesOption=calendarGrosses&sort=maxNumTheaters';
}

function compactWhitespaceV53_(s) {
  return String(s || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
}

function slugifyTitleV53_(s) {
  return String(s || '')
    .normalize('NFKD')
    .replace(/[^\w\s-]/g, ' ')
    .replace(/_/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\s/g, '-');
}

function escapeRegexV53_(s) {
  return String(s || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function normalizeSourceTypeLabel_(r) {
  var et = String((r && r.evidence_type) || '').toLowerCase();
  if (et === 'title_market_total') return 'title_market_total';
  if (et === 'title_weekly' || (r && r.record_semantics === 'title_period_gross')) return 'title_weekly';
  if (et === 'chart_mention') return 'chart_signal';
  if (et === 'release_footprint_only') return 'release_footprint';
  if (r && r.record_semantics === 'title_cumulative_total') return 'title_market_total';
  return et || 'other';
}

function inferOriginCountryCodeFromJobV53_(job) {
  var title = normalizeLatinTitle_(job && (job.matched_title || job.title_query || '') || '');
  if (title && title.indexOf('siko') >= 0) return 'EG';
  var releaseCodes = parseJsonArray_(job && job.release_markets_json || '');
  if (releaseCodes.indexOf('EG') >= 0) return 'EG';
  if (releaseCodes.indexOf('SA') >= 0) return 'SA';
  return '';
}

function guessTheNumbersCountryUrlsV53_(query, yearHint, targetMarketCodes, originCode) {
  var q = String(query || '').trim();
  var year = String(yearHint || '').trim();
  if (!q || !year) return [];
  var titleSlug = slugifyTitleV53_(q);
  var codes = unique_((targetMarketCodes || []).filter(Boolean));
  var originPart = '';
  if (originCode && CONFIG.MARKETS[originCode]) {
    // The Numbers often uses country of origin in the movie slug.
    var originCountry = CONFIG.MARKETS[originCode].country.replace(/\s+/g, '-');
    originPart = '-(' + originCountry + ')';
  }
  var bases = [
    'https://www.the-numbers.com/movie/' + titleSlug + '-(' + year + ')',
    originPart ? ('https://www.the-numbers.com/movie/' + titleSlug + originPart + '-(' + year + ')') : ''
  ].filter(Boolean);
  var out = [];
  codes.forEach(function(code) {
    var slug = findTheNumbersCountrySlug_(code);
    if (!slug) return;
    bases.forEach(function(base) {
      out.push(base + '/' + slug);
    });
  });
  return unique_(out);
}

function discoverTheNumbersCountryCandidates_(query, yearHint, targetMarketCodes, originCode) {
  var byUrl = {};
  var guessed = guessTheNumbersCountryUrlsV53_(query, yearHint, targetMarketCodes, originCode);
  guessed.forEach(function(url) {
    var normalized = normalizeTheNumbersCountryUrl_(url);
    if (!normalized) return;
    byUrl[normalized] = { url: normalized, score: 0.70, source: 'guessed' };
  });

  var codes = unique_((targetMarketCodes || []).filter(Boolean));
  if (!codes.length) codes = ['AE', 'KW', 'SA'];
  var searchQueries = [];
  codes.forEach(function(code) {
    var countrySlug = findTheNumbersCountrySlug_(code);
    var countryName = (CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || countrySlug.replace(/-/g, ' ');
    if (!countrySlug) return;
    searchQueries.push('site:the-numbers.com/movie/ "' + query + '" "' + countryName + '"');
    if (yearHint) searchQueries.push('site:the-numbers.com/movie/ "' + query + '" "' + countryName + '" "' + yearHint + '"');
  });
  unique_(searchQueries).forEach(function(sq) {
    searchEngineCandidates_(sq).forEach(function(hit) {
      var url = normalizeTheNumbersCountryUrl_(hit.url);
      if (!url) return;
      var score = scoreCandidateHit_(query, (hit.title || '') + ' ' + (hit.snippet || ''), yearHint);
      if (!byUrl[url] || score > byUrl[url].score) byUrl[url] = { url: url, score: score, source: 'search' };
    });
  });
  return Object.keys(byUrl)
    .sort(function(a, b) { return byUrl[b].score - byUrl[a].score; })
    .map(function(url) { return byUrl[url]; })
    .slice(0, 8);
}

function parseBoxOfficeMojoTitleAreaRows_(html, text, title, year, sourceUrl) {
  html = String(html || '');
  text = String(text || '');
  var rows = [];
  var seen = {};
  var compactText = compactWhitespaceV53_(text);
  var compactHtml = compactWhitespaceV53_(html);

  function pushRow(countryCode, releaseDate, opening, gross, parserConfidence, notes) {
    if (!countryCode || !gross) return;
    var country = (CONFIG.MARKETS[countryCode] && CONFIG.MARKETS[countryCode].country) || countryCode;
    var key = [countryCode, releaseDate || '', opening || '', gross].join('|');
    if (seen[key]) return;
    seen[key] = true;
    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_TITLE_MARKETS',
      source_url: sourceUrl || '',
      parser_confidence: parserConfidence || 0.84,
      source_entity_id: 'bom_title_area|' + key,
      country: country,
      country_code: countryCode,
      film_title_raw: title || '',
      film_title_ar_raw: '',
      release_year_hint: year || '',
      record_scope: 'title',
      record_granularity: 'lifetime',
      record_semantics: 'title_cumulative_total',
      source_confidence: 0.72,
      match_confidence: 0.92,
      evidence_type: 'title_market_total',
      period_label_raw: 'lifetime',
      period_start_date: releaseDate || '',
      period_end_date: '',
      period_key: '',
      rank: '',
      period_gross_local: opening || '',
      cumulative_gross_local: gross,
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor: '',
      notes: notes || 'Box Office Mojo title-page market total',
      raw_payload_json: JSON.stringify({ country_code: countryCode, release_date: releaseDate || '', opening_usd: opening || '', gross_usd: gross })
    }));
  }

  Object.keys(CONFIG.MARKETS).forEach(function(code) {
    var country = CONFIG.MARKETS[code].country;
    var cEsc = escapeRegexV53_(country).replace(/\s+/g, '\\s*');
    // Strategy 1: parsed text block (works with current public BOM rendering)
    var rxText = new RegExp(cEsc + '\\s*([A-Z][a-z]{2}\\.?\\s+\\d{1,2},\\s+20\\d{2})\\s*\\$([\\d,]+)\\s*\\$([\\d,]+)', 'ig');
    var m;
    while ((m = rxText.exec(compactText)) !== null) {
      pushRow(code, toIsoDateFromLong_(String(m[1] || '').replace(/\./g, '')), parseMoney_(m[2]), parseMoney_(m[3]), 0.90, 'Box Office Mojo title-page area row');
    }

    // Strategy 2: html table row fallback
    var rxHtml = new RegExp('>' + cEsc + '<[\\s\\S]{0,400}?>([A-Z][a-z]{2}\\.?\\s+\\d{1,2},\\s+20\\d{2})<[\\s\\S]{0,200}?>\\$([\\d,]+)<[\\s\\S]{0,120}?>\\$([\\d,]+)<', 'ig');
    while ((m = rxHtml.exec(compactHtml)) !== null) {
      pushRow(code, toIsoDateFromLong_(String(m[1] || '').replace(/\./g, '')), parseMoney_(m[2]), parseMoney_(m[3]), 0.86, 'Box Office Mojo title-page market row (html fallback)');
    }

    // Strategy 3: if only one dollar figure exists after earliest release date, keep as total-only
    var rxSingle = new RegExp(cEsc + '\\s*([A-Z][a-z]{2}\\.?\\s+\\d{1,2},\\s+20\\d{2})\\s*\\$([\\d,]+)(?!\\s*\\$)', 'ig');
    while ((m = rxSingle.exec(compactText)) !== null) {
      pushRow(code, toIsoDateFromLong_(String(m[1] || '').replace(/\./g, '')), '', parseMoney_(m[2]), 0.72, 'Box Office Mojo title-page market row (single-total fallback)');
    }
  });

  return dedupRawRecords_(rows);
}

function fetchBoxOfficeMojoYearMarketTotalsForTitle_(query, yearHint, countryCodes) {
  var rows = [];
  var year = String(yearHint || '').slice(0, 4);
  if (!year) return rows;
  unique_((countryCodes || []).filter(Boolean)).forEach(function(code) {
    try {
      var url = normalizeBoxOfficeMojoYearMarketUrlV53_(year, code);
      if (!url) return;
      var html = fetchUrl_(url);
      var text = htmlToText_(html);
      var lines = String(text || '').split(/\r?\n/).map(function(x) { return compactWhitespaceV53_(x); }).filter(Boolean);
      lines.forEach(function(line) {
        if (!titleMatchesQuery_(query, line, '', year, year)) return;
        var grosses = line.match(/\$[\d,]+/g) || [];
        if (!grosses.length) return;
        var total = parseMoney_(grosses[grosses.length - 1]);
        var releaseDateMatch = line.match(/([A-Z][a-z]{2}\s+\d{1,2})/);
        var releaseDate = releaseDateMatch ? toIsoDateFromLong_(releaseDateMatch[1] + ', ' + year) : '';
        rows.push(buildRawRecord_({
          source_name: 'BOXOFFICEMOJO_YEAR_MARKET_TOTALS',
          source_url: url,
          parser_confidence: 0.74,
          source_entity_id: 'bom_year|' + code + '|' + normalizeLatinTitle_(query) + '|' + year,
          country: CONFIG.MARKETS[code].country,
          country_code: code,
          film_title_raw: query,
          film_title_ar_raw: '',
          release_year_hint: year,
          record_scope: 'title',
          record_granularity: 'lifetime',
          record_semantics: 'title_cumulative_total',
          source_confidence: 0.60,
          match_confidence: 0.80,
          evidence_type: 'title_market_total',
          period_label_raw: 'calendar_year_' + year,
          period_start_date: releaseDate || '',
          period_end_date: '',
          period_key: '',
          rank: '',
          period_gross_local: '',
          cumulative_gross_local: total,
          currency: 'USD',
          admissions_actual: '',
          work_id: '',
          distributor: '',
          notes: 'Box Office Mojo yearly market page fallback; summary total only',
          raw_payload_json: JSON.stringify({ matched_line: line })
        }));
      });
    } catch (e) {}
  });
  return dedupRawRecords_(rows);
}

function fetchBoxOfficeMojoTitleCandidate_(titleUrl) {
  var normalized = normalizeBoxOfficeMojoTitleUrl_(titleUrl);
  if (!normalized) throw new Error('Invalid Box Office Mojo title URL');
  var html = fetchUrl_(normalized);
  var text = htmlToText_(html);

  var title = '';
  var year = '';
  var m = compactWhitespaceV53_(text).match(/#\s*(.*?)\s*\((20\d{2})\)/);
  if (m) {
    title = compactWhitespaceV53_(m[1]);
    year = m[2] || '';
  }
  if (!title) {
    var og = html.match(/<meta[^>]+property="og:title"[^>]+content="([^"]+)"/i);
    if (og) {
      var ogt = decodeHtmlEntitiesSimple_(og[1] || '');
      var mOg = ogt.match(/^(.*?)\s*\((20\d{2})\)/);
      if (mOg) {
        title = compactWhitespaceV53_(mOg[1]).trim();
        year = mOg[2] || year;
      }
    }
  }
  if (!title) {
    var h1 = html.match(/<h1[^>]*>\s*([^<]+?)\s*\((20\d{2})\)\s*<\/h1>/i);
    if (h1) {
      title = compactWhitespaceV53_(h1[1]);
      year = h1[2] || year;
    }
  }

  var releaseUrls = [];
  var rx = /(?:https:\/\/www\.boxofficemojo\.com)?(\/release\/rl\d+\/weekend\/?)/gi;
  var found;
  while ((found = rx.exec(html)) !== null) {
    var full = normalizeBoxOfficeMojoReleaseUrl_('https://www.boxofficemojo.com' + found[1]);
    if (full && releaseUrls.indexOf(full) < 0) releaseUrls.push(full);
  }

  var intlGross = '';
  var ig = compactWhitespaceV53_(text).match(/International\s+\(?100%\)?\s+\$([\d,]+)/i);
  if (ig) intlGross = parseMoney_(ig[1]);

  var areaRows = parseBoxOfficeMojoTitleAreaRows_(html, text, title, year, normalized);
  // If title page exposes only earliest release date and international gross, keep a UAE fallback row.
  if (!areaRows.length) {
    var earliest = compactWhitespaceV53_(text).match(/Earliest Release Date\s+([A-Z][a-z]+\s+\d{1,2},\s+20\d{2})\s+\(([^)]+)\)/i);
    if (earliest && intlGross) {
      var cCode = findMarketCodeByCountry_(earliest[2]);
      if (cCode) {
        areaRows = areaRows.concat(parseBoxOfficeMojoTitleAreaRows_(
          '',
          earliest[2] + ' ' + earliest[1] + ' $' + intlGross,
          title,
          year,
          normalized
        ));
      }
    }
  }

  return {
    title_url: normalized,
    title_en: title,
    release_year: year ? parseInt(year, 10) : '',
    release_urls: releaseUrls,
    intl_gross_usd: intlGross,
    area_rows: dedupRawRecords_(areaRows),
    source_url: normalized
  };
}

function buildBoxOfficeMojoTitleSummaryRows_(titleInfo) {
  if (!titleInfo) return [];
  var rows = [];
  if (titleInfo.area_rows && titleInfo.area_rows.length) rows = rows.concat(titleInfo.area_rows);
  if (titleInfo.title_en && titleInfo.intl_gross_usd) {
    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_TITLE_GLOBAL',
      source_url: titleInfo.source_url,
      parser_confidence: 0.80,
      source_entity_id: titleInfo.title_url || titleInfo.source_url,
      country: 'All Territories',
      country_code: '',
      film_title_raw: titleInfo.title_en,
      film_title_ar_raw: '',
      release_year_hint: titleInfo.release_year || '',
      record_scope: 'global',
      record_granularity: 'lifetime',
      record_semantics: 'title_cumulative_total',
      source_confidence: 0.58,
      match_confidence: 0.88,
      evidence_type: 'title_market_total',
      period_label_raw: 'lifetime',
      period_start_date: '',
      period_end_date: '',
      period_key: '',
      rank: '',
      period_gross_local: '',
      cumulative_gross_local: titleInfo.intl_gross_usd,
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor: '',
      notes: 'International total from Box Office Mojo title page; global evidence only',
      raw_payload_json: JSON.stringify(titleInfo)
    }));
  }
  return dedupRawRecords_(rows);
}

function lookupStageBomTitle_(ss, job, query, yearHint) {
  var discovered = parseJsonArray_(job.discovered_title_urls_json);
  var processed = parseJsonArray_(job.processed_title_urls_json);
  var discoveredReleaseUrls = parseJsonArray_(job.discovered_release_urls_json);
  var next = discovered.filter(function(url) { return processed.indexOf(url) < 0; })[0];
  if (!next) {
    job.stage = 'BOM_RELEASE';
    job.updated_at = isoNow_();
    upsertLookupJob_(ss, job);
    return { progressed: true, dirtyRecon: false };
  }

  var marketRows = [];
  var globalRows = [];
  try {
    var titleInfo = fetchBoxOfficeMojoTitleCandidate_(next);
    var allRows = (buildBoxOfficeMojoTitleSummaryRows_(titleInfo) || []).filter(function(r) {
      return titleMatchesQuery_(query, r.film_title_raw, r.film_title_ar_raw || '', r.release_year_hint, yearHint);
    });
    marketRows = allRows.filter(function(r) { return r.country_code; });
    globalRows = allRows.filter(function(r) { return !r.country_code; });

    var existingMarketCodes = unique_(marketRows.map(function(r) { return r.country_code; }).filter(Boolean));
    var fallbackCodes = unique_(['AE', 'KW', 'SA'].concat(parseJsonArray_(job.release_markets_json).filter(function(c) {
      return ['AE', 'KW', 'SA'].indexOf(c) >= 0;
    }))).filter(function(code) {
      return existingMarketCodes.indexOf(code) < 0;
    });

    if (fallbackCodes.length) {
      var fallbackYearRows = fetchBoxOfficeMojoYearMarketTotalsForTitle_(job.matched_title || query, yearHint || job.matched_release_year || '', fallbackCodes);
      if (fallbackYearRows.length) {
        marketRows = marketRows.concat(fallbackYearRows);
        allRows = allRows.concat(fallbackYearRows);
      }
    }

    discoveredReleaseUrls = unique_(discoveredReleaseUrls.concat((titleInfo.release_urls || []).filter(Boolean)));
    job.discovered_release_urls_json = JSON.stringify(discoveredReleaseUrls);

    appendJobSourceStatus_(ss, job, {
      source: 'BOXOFFICEMOJO_TITLE_MARKETS',
      status: 'OK',
      rows: marketRows.length,
      notes: marketRows.length ? ('Fetched Box Office Mojo market-total evidence for: ' + unique_(marketRows.map(function(r) { return r.country_code; })).join(', ')) : 'Title page/year-market fallback yielded no title-market rows'
    });
    appendJobSourceStatus_(ss, job, {
      source: 'BOXOFFICEMOJO_TITLE_GLOBAL',
      status: 'OK',
      rows: globalRows.length,
      notes: globalRows.length ? 'Fetched Box Office Mojo global title-summary evidence' : 'No usable global-only title summary row'
    });

    if (allRows.length) {
      persistRawRecordsForJob_(ss, job, allRows);
      autopromoteFilmFromRows_(ss, job, allRows);
      job.dirty_recon = 'yes';
    } else {
      appendJobAnomaly_(ss, job, 'Box Office Mojo title page resolved with zero usable title-summary rows: ' + next);
    }
  } catch (e) {
    appendJobSourceStatus_(ss, job, { source: 'BOXOFFICEMOJO_TITLE_MARKETS', status: 'ERROR', rows: 0, notes: e.message });
    appendJobAnomaly_(ss, job, 'Box Office Mojo title page failed for ' + next + ': ' + e.message);
  }

  processed.push(next);
  job.processed_title_urls_json = JSON.stringify(unique_(processed));
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
  return { progressed: true, dirtyRecon: marketRows.length + globalRows.length > 0 };
}

function lookupStageBomRelease_(ss, job, query, yearHint) {
  var discovered = parseJsonArray_(job.discovered_release_urls_json);
  var processed = parseJsonArray_(job.processed_release_urls_json);
  var next = discovered.filter(function(url) { return processed.indexOf(url) < 0; })[0];

  var relRows = [];
  if (next) {
    try {
      relRows = (fetchBoxOfficeMojoReleaseEvidence_(next, query, yearHint) || []).filter(function(r) {
        return titleMatchesQuery_(query, r.film_title_raw, r.film_title_ar_raw || '', r.release_year_hint, yearHint);
      });
      appendJobSourceStatus_(ss, job, {
        source: 'BOXOFFICEMOJO_RELEASE',
        status: 'OK',
        rows: relRows.length,
        notes: relRows.length ? 'Fetched territory-specific release evidence' : 'Release page resolved but yielded no usable rows'
      });
      if (!relRows.length) appendJobAnomaly_(ss, job, 'Box Office Mojo release page resolved with zero usable rows: ' + next);
    } catch (e) {
      appendJobSourceStatus_(ss, job, { source: 'BOXOFFICEMOJO_RELEASE', status: 'ERROR', rows: 0, notes: e.message });
      appendJobAnomaly_(ss, job, 'Box Office Mojo release page failed for ' + next + ': ' + e.message);
    }
    processed.push(next);
    job.processed_release_urls_json = JSON.stringify(unique_(processed));
  } else {
    appendJobSourceStatus_(ss, job, {
      source: 'BOXOFFICEMOJO_RELEASE',
      status: 'OK',
      rows: 0,
      notes: 'No Box Office Mojo release URLs discovered for this title'
    });
  }

  var targetMarkets = lookupPriorityMarkets_().filter(function(code) {
    return ['AE', 'KW', 'SA'].indexOf(code) >= 0;
  });
  var numbersRows = [];
  try {
    var nums = discoverTheNumbersCountryCandidates_(job.matched_title || query, yearHint || job.matched_release_year || '', targetMarkets, inferOriginCountryCodeFromJobV53_(job));
    nums.forEach(function(hit) {
      var hitRows = fetchTheNumbersCountryEvidenceByUrl_(hit.url, job.matched_title || query, yearHint || job.matched_release_year || '');
      if (hitRows && hitRows.length) numbersRows = numbersRows.concat(hitRows);
    });
    appendJobSourceStatus_(ss, job, {
      source: 'THENUMBERS_COUNTRY_PAGE',
      status: 'OK',
      rows: numbersRows.length,
      notes: numbersRows.length ? ('Fetched The Numbers country totals for: ' + unique_(numbersRows.map(function(r) { return r.country_code; })).join(', ')) : ('No usable The Numbers country-page totals found after ' + nums.length + ' candidate attempts')
    });
  } catch (e2) {
    appendJobSourceStatus_(ss, job, { source: 'THENUMBERS_COUNTRY_PAGE', status: 'ERROR', rows: 0, notes: e2.message });
    appendJobAnomaly_(ss, job, 'The Numbers country-page fetch failed: ' + e2.message);
  }

  var allRows = dedupRawRecords_(relRows.concat(numbersRows));
  if (allRows.length) {
    persistRawRecordsForJob_(ss, job, allRows);
    autopromoteFilmFromRows_(ss, job, allRows);
    job.dirty_recon = 'yes';
  }

  job.stage = 'SUPPLEMENTAL_CHARTS';
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
  return { progressed: true, dirtyRecon: allRows.length > 0 };
}

function inferFreshnessModeLabel_(job) {
  if (!job) return [{ source: 'NO_JOB', status: 'No lookup job found', rows: '', notes: 'No matching queued or completed lookup job for this title.' }];
  var statusList = parseJsonArray_(job.source_status_json);
  var successful = statusList.filter(function(s) { return String(s.status || '').toUpperCase() === 'OK'; });
  var lifecycle = String(job.status || '').toUpperCase() || 'UNKNOWN';
  if (!successful.length) {
    return [{
      source: 'CACHE_ONLY',
      status: lifecycle || 'UNKNOWN',
      rows: '',
      notes: 'No successful live source hits recorded. Report rendered from existing local reconciled data.'
    }];
  }
  return [{
    source: 'LIVE_RUN',
    status: lifecycle,
    rows: successful.reduce(function(sum, s) { return sum + Number(s.rows || 0); }, 0),
    notes: 'Report rendered from cached live retrieval dataset. Last live retrieval: ' + (job.updated_at || '')
  }].concat(successful.map(function(s) {
    return { source: s.source, status: s.status, rows: s.rows, notes: s.notes };
  }));
}


// ============================================================
// V5.4 FINAL HARDENING PATCH
// - fixes BOM title/year market-total parsing correctness
// - makes The Numbers country fallback non-fatal and search-led
// - improves source concentration wording
// - improves queue/render finalization so lookup reflects completed jobs
// ============================================================

function monthRegexV54_() {
  return '(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)';
}

function parseBoxOfficeMojoTitleAreaRows_(html, text, title, year, sourceUrl) {
  html = String(html || '');
  text = String(text || '');
  var rows = [];
  var seen = {};
  var compactText = compactWhitespaceV53_(text);
  var compactHtml = compactWhitespaceV53_(html);
  var monthRx = monthRegexV54_();

  function pushRow(countryCode, releaseDate, opening, gross, parserConfidence, notes) {
    opening = coerceNumberOrBlank_(opening, '');
    gross = coerceNumberOrBlank_(gross, '');
    if (!countryCode || !gross || Number(gross) <= 0) return;
    var country = (CONFIG.MARKETS[countryCode] && CONFIG.MARKETS[countryCode].country) || countryCode;
    var key = [countryCode, releaseDate || '', opening || '', gross].join('|');
    if (seen[key]) return;
    seen[key] = true;
    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_TITLE_MARKETS',
      source_url: sourceUrl || '',
      parser_confidence: parserConfidence || 0.84,
      source_entity_id: 'bom_title_area|' + key,
      country: country,
      country_code: countryCode,
      film_title_raw: title || '',
      film_title_ar_raw: '',
      release_year_hint: year || '',
      record_scope: 'title',
      record_granularity: 'lifetime',
      record_semantics: 'title_cumulative_total',
      source_confidence: 0.78,
      match_confidence: 0.94,
      evidence_type: 'title_market_total',
      period_label_raw: 'lifetime',
      period_start_date: releaseDate || '',
      period_end_date: '',
      period_key: '',
      rank: '',
      period_gross_local: opening || '',
      cumulative_gross_local: gross,
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor: '',
      notes: notes || 'Box Office Mojo title-page market total',
      raw_payload_json: JSON.stringify({ country_code: countryCode, release_date: releaseDate || '', opening_usd: opening || '', gross_usd: gross })
    }));
  }

  Object.keys(CONFIG.MARKETS).forEach(function(code) {
    var country = CONFIG.MARKETS[code].country;
    var cEsc = escapeRegexV53_(country).replace(/\s+/g, '\\s*');
    var patterns = [
      new RegExp(cEsc + '\\s*(' + monthRx + '\\.?\\s+\\d{1,2},\\s+20\\d{2})\\s*\\$([\\d,]+)\\s*\\$([\\d,]+)', 'ig'),
      new RegExp(cEsc + '[\\s\\S]{0,120}?(' + monthRx + '\\.?\\s+\\d{1,2},\\s+20\\d{2})[\\s\\S]{0,80}?\\$([\\d,]+)[\\s\\S]{0,80}?\\$([\\d,]+)', 'ig'),
      new RegExp(cEsc + '\\s*(' + monthRx + '\\.?\\s+\\d{1,2},\\s+20\\d{2})\\s*\\$([\\d,]+)(?!\\s*\\$)', 'ig')
    ];

    patterns.forEach(function(rx, idx) {
      var target = idx === 1 ? compactHtml : compactText;
      var m;
      while ((m = rx.exec(target)) !== null) {
        var dateStr = String(m[1] || '').replace(/\./g, '');
        var releaseDate = toIsoDateFromLong_(dateStr);
        var firstDollar = parseMoney_(m[2]);
        var secondDollar = parseMoney_(m[3]);
        var opening = '';
        var gross = '';
        if (secondDollar && secondDollar >= firstDollar) {
          opening = firstDollar;
          gross = secondDollar;
        } else {
          gross = firstDollar;
        }
        pushRow(code, releaseDate, opening, gross, idx === 0 ? 0.92 : (idx === 1 ? 0.88 : 0.74), idx === 0 ? 'Box Office Mojo title-page area row' : (idx === 1 ? 'Box Office Mojo title-page html market row' : 'Box Office Mojo title-page single-total fallback'));
      }
    });
  });

  return dedupRawRecords_(rows);
}

function fetchBoxOfficeMojoYearMarketTotalsForTitle_(query, yearHint, countryCodes) {
  var rows = [];
  var year = String(yearHint || '').slice(0, 4);
  if (!year) return rows;

  var qNorm = normalizeLatinTitle_(query);

  unique_((countryCodes || []).filter(Boolean)).forEach(function(code) {
    try {
      var url = normalizeBoxOfficeMojoYearMarketUrlV53_(year, code);
      if (!url) return;

      var html = fetchUrl_(url);
      var text = htmlToText_(html);

      var lines = String(text || '')
        .split(/\r?\n/)
        .map(function(x) { return compactWhitespaceV53_(x); })
        .filter(Boolean);

      var best = null;

      lines.forEach(function(line) {
        var nLine = normalizeLatinTitle_(line);
        if (!qNorm || nLine.indexOf(qNorm) < 0) return;

        var anchored = new RegExp('^(\\d+\\s+)?' + escapeRegexV53_(qNorm) + '(\\s|$)').test(nLine);
        if (!anchored) return;

        var dollars = line.match(/\$[\d,]+(?:\.\d+)?/g) || [];
        if (!dollars.length) return;

        var values = dollars
          .map(function(d) { return parseMoney_(d); })
          .filter(function(v) { return Number(v) > 0; });

        if (!values.length) return;

        var total = values[values.length - 1];
        var openingOrGross = values.length > 1 ? values[0] : '';

        var relMatch = line.match(/((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2})/i);
        var relDate = relMatch ? toIsoDateFromLong_(relMatch[1] + ', ' + year) : '';

        if (openingOrGross && total && total < openingOrGross) return;
        if (openingOrGross && total && openingOrGross < 1000 && total > 5000000) return;

        var score = 0.70;
        if (relDate) score += 0.08;
        if (values.length >= 2) score += 0.06;

        if (!best || score > best.score || (score === best.score && total > best.total)) {
          best = {
            line: line,
            total: total,
            opening: openingOrGross,
            releaseDate: relDate,
            score: score
          };
        }
      });

      if (best && best.total) {
        rows.push(buildRawRecord_({
          source_name: 'BOXOFFICEMOJO_YEAR_MARKET_TOTALS',
          source_url: url,
          parser_confidence: Math.min(0.92, best.score),
          source_entity_id: 'bom_year|' + code + '|' + normalizeLatinTitle_(query) + '|' + year,
          country: CONFIG.MARKETS[code].country,
          country_code: code,
          film_title_raw: query,
          film_title_ar_raw: '',
          release_year_hint: year,
          record_scope: 'title',
          record_granularity: 'lifetime',
          record_semantics: 'title_cumulative_total',
          source_confidence: 0.66,
          match_confidence: 0.88,
          evidence_type: 'title_market_total',
          period_label_raw: 'calendar_year_' + year,
          period_start_date: best.releaseDate || '',
          period_end_date: '',
          period_key: '',
          rank: '',
          period_gross_local: best.opening || '',
          cumulative_gross_local: best.total,
          currency: 'USD',
          admissions_actual: '',
          work_id: '',
          distributor: '',
          notes: 'Box Office Mojo yearly market page fallback; exact row anchored',
          raw_payload_json: JSON.stringify({ matched_line: best.line })
        }));
      }

    } catch (e) {}
  });

  return dedupRawRecords_(rows);
}

function lookupStageBomRelease_(ss, job, query, yearHint) {
  var discovered = parseJsonArray_(job.discovered_release_urls_json);
  var processed = parseJsonArray_(job.processed_release_urls_json);
  var next = discovered.filter(function(url) { return processed.indexOf(url) < 0; })[0];

  var relRows = [];
  if (next) {
    try {
      relRows = (fetchBoxOfficeMojoReleaseEvidence_(next, query, yearHint) || []).filter(function(r) {
        return titleMatchesQuery_(query, r.film_title_raw, r.film_title_ar_raw || '', r.release_year_hint, yearHint);
      });
      appendJobSourceStatus_(ss, job, {
        source: 'BOXOFFICEMOJO_RELEASE',
        status: 'OK',
        rows: relRows.length,
        notes: relRows.length ? 'Fetched territory-specific release evidence' : 'Release page resolved but yielded no usable rows'
      });
      if (!relRows.length) appendJobAnomaly_(ss, job, 'Box Office Mojo release page resolved with zero usable rows: ' + next);
    } catch (e) {
      appendJobSourceStatus_(ss, job, { source: 'BOXOFFICEMOJO_RELEASE', status: 'ERROR', rows: 0, notes: e.message });
      appendJobAnomaly_(ss, job, 'Box Office Mojo release page failed for ' + next + ': ' + e.message);
    }
    processed.push(next);
    job.processed_release_urls_json = JSON.stringify(unique_(processed));
  } else {
    appendJobSourceStatus_(ss, job, {
      source: 'BOXOFFICEMOJO_RELEASE',
      status: 'OK',
      rows: 0,
      notes: 'No Box Office Mojo release URLs discovered for this title'
    });
  }

  var targetMarkets = lookupPriorityMarkets_().filter(function(code) {
    return ['AE', 'KW', 'SA'].indexOf(code) >= 0;
  });
  var numbersRows = [];
  var candidateCount = 0;
  var numbersErrors = [];
  var nums = [];
  try {
    nums = discoverTheNumbersCountryCandidates_(job.matched_title || query, yearHint || job.matched_release_year || '', targetMarkets, inferOriginCountryCodeFromJobV53_(job));
    candidateCount = nums.length;
    nums.forEach(function(hit) {
      try {
        var hitRows = fetchTheNumbersCountryEvidenceByUrl_(hit.url, job.matched_title || query, yearHint || job.matched_release_year || '');
        if (hitRows && hitRows.length) numbersRows = numbersRows.concat(hitRows);
      } catch (inner) {
        var msg = String(inner && inner.message || inner || '');
        if (msg.indexOf('404') < 0) numbersErrors.push(msg);
      }
    });
    appendJobSourceStatus_(ss, job, {
      source: 'THENUMBERS_COUNTRY_PAGE',
      status: numbersRows.length ? 'OK' : (numbersErrors.length ? 'WARN' : 'OK'),
      rows: numbersRows.length,
      notes: numbersRows.length ? ('Fetched The Numbers country totals for: ' + unique_(numbersRows.map(function(r) { return r.country_code; })).join(', ')) : ('No usable The Numbers country-page totals found after ' + candidateCount + ' candidate attempts')
    });
    if (numbersErrors.length) appendJobAnomaly_(ss, job, 'The Numbers non-404 fetch issues: ' + unique_(numbersErrors).slice(0, 3).join(' | '));
  } catch (e2) {
    appendJobSourceStatus_(ss, job, { source: 'THENUMBERS_COUNTRY_PAGE', status: 'WARN', rows: 0, notes: e2.message });
  }

  var allRows = dedupRawRecords_(relRows.concat(numbersRows));
  if (allRows.length) {
    persistRawRecordsForJob_(ss, job, allRows);
    autopromoteFilmFromRows_(ss, job, allRows);
    job.dirty_recon = 'yes';
  }

  job.stage = 'SUPPLEMENTAL_CHARTS';
  job.updated_at = isoNow_();
  upsertLookupJob_(ss, job);
  return { progressed: true, dirtyRecon: allRows.length > 0 };
}

function buildAcquisitionSummary_(rows, releaseMarkets, match, statuses) {
  var titleRows = rows.filter(function(r) { return r.record_scope === 'title'; });
  var weeklyRows = titleRows.filter(function(r) { return r.record_semantics === 'title_period_gross'; });
  var cumulativeRows = titleRows.filter(function(r) { return r.record_semantics === 'title_cumulative_total'; });
  var weeklyMarkets = unique_(weeklyRows.map(function(r) { return r.country_code; }).filter(Boolean));
  var perfMarkets = unique_(titleRows.map(function(r) { return r.country_code; }).filter(Boolean));
  var releaseCodes = unique_((releaseMarkets || []).map(function(x) { return x.country_code || x; }).filter(Boolean));
  var missing = lookupPriorityMarkets_().filter(function(code) { return perfMarkets.indexOf(code) < 0; });
  var best = rankBestMarkets_(weeklyRows, cumulativeRows).slice(0, 3);
  var sourceCounts = {};
  titleRows.forEach(function(r) { sourceCounts[r.source_name] = (sourceCounts[r.source_name] || 0) + 1; });
  var weeklySourceCounts = {};
  weeklyRows.forEach(function(r) { weeklySourceCounts[r.source_name] = (weeklySourceCounts[r.source_name] || 0) + 1; });
  var totalSources = Object.keys(sourceCounts).length;
  var totalWeeklySources = Object.keys(weeklySourceCounts).length;
  var topWeeklySource = Object.keys(weeklySourceCounts).sort(function(a, b) { return weeklySourceCounts[b] - weeklySourceCounts[a]; })[0] || '';
  var supplementalSources = Object.keys(sourceCounts).filter(function(k) { return !weeklySourceCounts[k]; });

  var strength = 'Weak';
  if (weeklyMarkets.length >= 3 && totalSources >= 2) strength = 'Strong';
  else if (weeklyMarkets.length >= 2 || (weeklyMarkets.length >= 1 && perfMarkets.length >= 3)) strength = 'Moderate';
  else if (titleRows.length) strength = 'Limited';

  var take = 'Insufficient evidence for a confident acquisition read.';
  if (weeklyRows.length) {
    var byCountry = {};
    weeklyRows.forEach(function(r) {
      if (!byCountry[r.country_code]) byCountry[r.country_code] = [];
      byCountry[r.country_code].push(r);
    });
    var rankedCodes = Object.keys(byCountry).sort(function(a, b) { return byCountry[b].length - byCountry[a].length; });
    var parts = [];
    rankedCodes.slice(0, 2).forEach(function(code) {
      var list = byCountry[code].slice().sort(function(a, b) { return periodSortValue_(a) - periodSortValue_(b); });
      var peak = 0;
      list.forEach(function(r) { peak = Math.max(peak, Number(r.period_gross_local || 0)); });
      parts.push((CONFIG.MARKETS[code] ? CONFIG.MARKETS[code].country : code) + ' shows ' + list.length + ' tracked weeks and a peak weekly gross of ~' + formatNumberShort_(peak) + ' ' + (list[0] ? (list[0].currency || '') : ''));
    });
    var cumulativeMarkets = unique_(cumulativeRows.filter(function(r) { return weeklyMarkets.indexOf(r.country_code) < 0 && r.country_code; }).map(function(r) { return r.country_code; }));
    if (cumulativeMarkets.length) {
      parts.push('Additional summary-level market totals exist for ' + cumulativeMarkets.map(function(code) { return CONFIG.MARKETS[code] ? CONFIG.MARKETS[code].country : code; }).join(', ') + '.');
    }
    take = parts.join('. ') + '.';
  } else if (cumulativeRows.length) {
    take = 'Only title-summary market totals are available. This is helpful for sizing territories but weak for run-quality analysis.';
  }

  var sourceConcentration = 'None';
  if (weeklyRows.length) {
    sourceConcentration = totalWeeklySources ? (
      'Weekly evidence concentrated in ' + totalWeeklySources + ' source' + (totalWeeklySources > 1 ? 's' : '') +
      (topWeeklySource ? ('; primary weekly source: ' + topWeeklySource + ' (' + weeklySourceCounts[topWeeklySource] + ' rows)') : '') +
      (supplementalSources.length ? ('; supplemental summary sources: ' + supplementalSources.join(', ')) : '')
    ) : 'None';
  } else if (totalSources) {
    sourceConcentration = 'No weekly evidence. Summary-only evidence from ' + totalSources + ' source' + (totalSources > 1 ? 's' : '') + '.';
  }

  return {
    matched_film: (match && match.title) || (titleRows[0] && (titleRows[0].canonical_title || titleRows[0].film_title_raw)) || '',
    match_confidence: (match && match.score) || '',
    identity_status: (match && match.film_id) ? 'matched' : 'unmatched',
    release_year: (match && match.release_year) || (titleRows[0] && (titleRows[0].release_year || titleRows[0].release_year_hint)) || '',
    evidence_strength: strength,
    best_markets: best.join(', ') || 'None',
    missing_priority_markets: missing.join(', ') || 'None',
    weekly_markets: weeklyMarkets.join(', ') || 'None',
    source_concentration: sourceConcentration,
    analyst_take: take
  };
}

function inferFreshnessModeLabel_(job) {
  if (!job) return [{ source: 'NO_JOB', status: 'No lookup job found', rows: '', notes: 'No matching queued or completed lookup job for this title.' }];
  var statusList = parseJsonArray_(job.source_status_json);
  var successful = statusList.filter(function(s) { return String(s.status || '').toUpperCase() === 'OK'; });
  var lifecycle = String(job.status || '').toUpperCase() || 'UNKNOWN';
  var note = 'Report rendered from cached live retrieval dataset. Last live retrieval: ' + (job.updated_at || '');
  if (!successful.length) {
    return [{ source: 'CACHE_ONLY', status: lifecycle, rows: '', notes: 'No successful live source hits recorded. ' + note }];
  }
  return [{ source: 'LIVE_RUN', status: lifecycle, rows: successful.reduce(function(sum, s) { return sum + Number(s.rows || 0); }, 0), notes: note }].concat(successful.map(function(s) {
    return { source: s.source, status: s.status, rows: s.rows, notes: s.notes };
  }));
}

function processLookupJobBudgetById_(ss, jobId, budgetMs) {
  var start = Date.now();
  var dirtyRecon = false;
  while ((Date.now() - start) < budgetMs) {
    var job = getLookupJobById_(ss, jobId);
    if (!job) break;
    if (job.status === 'complete' || job.status === 'failed' || job.stage === 'COMPLETE') break;
    var step = processSingleLookupJobStage_(ss, job);
    dirtyRecon = dirtyRecon || step.dirtyRecon;
    if (!step.progressed) break;
    if ((Date.now() - start) > (budgetMs - 2500)) break;
  }

  if (dirtyRecon) rebuildReconciledEvidence_(ss);

  var finalJob = getLookupJobById_(ss, jobId);
  if (!finalJob) return;
  if (String(finalJob.stage || '').toUpperCase() === 'COMPLETE' && String(finalJob.status || '').toLowerCase() !== 'complete') {
    finalJob.status = 'complete';
    finalJob.updated_at = isoNow_();
    upsertLookupJob_(ss, finalJob);
  }
  if (finalJob.active_lookup === 'yes') {
    var sheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
    var currentQuery = String(sheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '').trim();
    if (normalizeLatinTitle_(currentQuery) === normalizeLatinTitle_(finalJob.title_query || '')) {
      renderLookup_(currentQuery, false);
      var latest = getLatestLookupJobByQuery_(ss, currentQuery) || finalJob;
      writeLookupJobBanner_(ss, latest, String(latest.status || 'queued').toUpperCase());
      writeLookupJobFailureNotice_(ss, latest);
    }
  }
}


// ============================================================
// V5.4.1 BOM RELEASE WEEKEND ENHANCEMENT PATCH
// - canonicalizes BOM release URLs to /weekend/
// - discovers release URLs from title pages even when only base /release/ links exist
// - parses full weekend runs from BOM release weekend pages for UAE and other markets
// - preserves lifetime summary rows and working existing features
// ============================================================

function normalizeBoxOfficeMojoReleaseUrl_(url) {
  var s = String(url || '').trim();
  var m = s.match(/https?:\/\/www\.boxofficemojo\.com\/release\/(rl\d+)\/?(?:weekend\/?)?/i) ||
          s.match(/\/release\/(rl\d+)\/?(?:weekend\/?)?/i);
  return m ? ('https://www.boxofficemojo.com/release/' + m[1] + '/weekend/') : '';
}

function extractBoxOfficeMojoReleaseUrlsFromHtmlV541_(html) {
  html = String(html || '');
  var out = [];
  var seen = {};
  var rx = /(?:https:\/\/www\.boxofficemojo\.com)?(\/release\/rl\d+\/?(?:weekend\/?)?[^"' <>\s]*)/gi;
  var m;
  while ((m = rx.exec(html)) !== null) {
    var full = m[1].indexOf('http') === 0 ? m[1] : ('https://www.boxofficemojo.com' + m[1]);
    var norm = normalizeBoxOfficeMojoReleaseUrl_(full);
    if (norm && !seen[norm]) {
      seen[norm] = true;
      out.push(norm);
    }
  }
  return out;
}


function inferBoxOfficeMojoReleaseCountryV543_(html, text, url) {
  html = String(html || '');
  text = String(text || '');
  url = String(url || '');

  var adjectiveMap = {
    'Emirati': 'AE',
    'Saudi': 'SA',
    'Kuwaiti': 'KW',
    'Bahraini': 'BH',
    'Omani': 'OM',
    'Jordanian': 'JO',
    'Lebanese': 'LB',
    'Qatari': 'QA',
    'Egyptian': 'EG'
  };

  function toResult(code) {
    return code ? {
      code: code,
      country: (CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code
    } : { code: '', country: '' };
  }

  var blob = compactWhitespaceV53_(text + ' ' + decodeHtmlEntitiesSimple_(html.replace(/<[^>]+>/g, ' ')));

  var mt = blob.match(/All Territories\s+([A-Za-z][A-Za-z ]{2,60})\s+Grosses/i);
  if (mt) {
    var c = findMarketCodeByCountry_(compactWhitespaceV53_(mt[1]));
    if (c) return toResult(c);
  }

  mt = blob.match(/Grosses\s+([A-Za-z][A-Za-z ]{2,60})\s+\$[\d,]+/i);
  if (mt) {
    var c2 = findMarketCodeByCountry_(compactWhitespaceV53_(mt[1]));
    if (c2) return toResult(c2);
  }

  var keys = Object.keys(adjectiveMap);
  for (var i = 0; i < keys.length; i++) {
    var adj = keys[i];
    if (new RegExp('\\b' + escapeRegexV53_(adj) + '\\s+(?:Weekend|Weekly|Daily)\\b', 'i').test(blob)) {
      return toResult(adjectiveMap[adj]);
    }
  }

  Object.keys(CONFIG.MARKETS).forEach(function(code) {
    if (mt) return;
    var cname = CONFIG.MARKETS[code].country;
    if (new RegExp('\\b' + escapeRegexV53_(cname) + '\\b', 'i').test(blob)) mt = { found: code };
  });
  if (mt && mt.found) return toResult(mt.found);

  return toResult('');
}

function extractBoxOfficeMojoReleaseTitleV543_(html, text, query) {
  html = String(html || '');
  text = String(text || '');
  query = String(query || '').trim();

  var title = '';
  var year = '';

  var h1 = html.match(/<h1[^>]*>\s*([^<]+?)\s*<\/h1>/i);
  if (h1) {
    title = compactWhitespaceV53_(decodeHtmlEntitiesSimple_(h1[1]));
    var m1 = title.match(/^(.*?)\s*\((20\d{2})\)$/);
    if (m1) {
      title = compactWhitespaceV53_(m1[1]);
      year = m1[2] || '';
    }
  }

  if (!title) {
    var og = html.match(/<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']/i);
    if (og) {
      var ogt = compactWhitespaceV53_(decodeHtmlEntitiesSimple_(og[1]));
      ogt = ogt.replace(/\s*[-|]\s*Box Office Mojo.*$/i, '').trim();
      var mog = ogt.match(/^(.*?)\s*\((20\d{2})\)$/);
      if (mog) {
        title = compactWhitespaceV53_(mog[1]);
        year = mog[2] || '';
      } else {
        title = ogt;
      }
    }
  }

  if (!title) {
    var blob = String(text || '').replace(/\r/g, '');
    var lines = blob.split(/\n/).map(function(x) { return compactWhitespaceV53_(x); }).filter(Boolean);
    for (var i = 0; i < lines.length; i++) {
      if (/Title Summary/i.test(lines[i]) && i > 0) {
        title = compactWhitespaceV53_(lines[i - 1]).replace(/^#\s*/, '');
        break;
      }
    }
  }

  if (!title) title = query || '';
  title = title.replace(/\s*[-|]\s*Box Office Mojo.*$/i, '').replace(/^#\s*/, '').trim();
  return { title: title, year: year };
}


function parseBoxOfficeMojoReleaseWeekendRowsV541_(html, text, meta) {
  html = String(html || '');
  text = String(text || '');
  meta = meta || {};

  var title = meta.title || '';
  var year = meta.year || '';
  var countryCode = meta.countryCode || '';
  var country = meta.country || ((countryCode && CONFIG.MARKETS[countryCode]) ? CONFIG.MARKETS[countryCode].country : '');
  var url = meta.url || '';
  var releaseDate = meta.releaseDate || '';
  var gross = meta.gross || '';

  var rows = [];
  var seen = {};

  function pushWeekendRow(dateLabel, rank, weekendGross, toDate, theaters, avg, pctLW, changeVal, weekendNumber, estimatedFlag, rawCells) {
    if (!dateLabel || !weekendGross) return;
    var effectiveCode = countryCode || meta.countryCode || '';
    var effectiveCountry = country || ((effectiveCode && CONFIG.MARKETS[effectiveCode]) ? CONFIG.MARKETS[effectiveCode].country : '');
    if (!effectiveCode) return;

    var fallbackYear = releaseDate ? parseInt(String(releaseDate).slice(0, 4), 10) : (parseInt(year, 10) || new Date().getFullYear());
    var period = parseMojoReleaseWeekendLabelPatched_(dateLabel, fallbackYear);
    var key = [effectiveCode, period ? period.key : dateLabel, weekendGross, toDate || '', rank || ''].join('|');
    if (seen[key]) return;
    seen[key] = true;

    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_RELEASE',
      source_url: url,
      parser_confidence: 0.97,
      source_entity_id: url + '|weekend|' + (period ? period.key : dateLabel),
      country: effectiveCountry,
      country_code: effectiveCode,
      film_title_raw: title,
      film_title_ar_raw: '',
      release_year_hint: year || '',
      record_scope: 'title',
      record_granularity: 'weekend',
      record_semantics: 'title_period_gross',
      source_confidence: 0.88,
      match_confidence: 0.96,
      evidence_type: 'title_performance',
      period_label_raw: dateLabel,
      period_start_date: period ? period.start : '',
      period_end_date: period ? period.end : '',
      period_key: period ? period.key : '',
      rank: rank || '',
      period_gross_local: weekendGross || '',
      cumulative_gross_local: toDate || gross || '',
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor: '',
      notes: 'Weekend row parsed from Box Office Mojo release weekend page',
      raw_payload_json: JSON.stringify({
        date_label: dateLabel,
        rank: rank || '',
        theaters: theaters || '',
        avg: avg || '',
        pct_lw: pctLW || '',
        change: changeVal || '',
        weekend_number: weekendNumber || '',
        estimated: estimatedFlag || '',
        cells: rawCells || []
      })
    }));
  }

  var candidates = [];
  var textLines = String(text || '').split(/\r?\n/).map(function(x) { return compactWhitespaceV53_(x); }).filter(Boolean);
  candidates = candidates.concat(textLines);

  candidates.push(
    compactWhitespaceV53_(String(text || '')
      .replace(/[【】]/g, ' ')
      .replace(/\b\d+[†‡]\b/g, ' ')
      .replace(/[†‡]/g, ' '))
  );

  candidates.push(
    compactWhitespaceV53_(decodeHtmlEntitiesSimple_(String(html || '').replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<style[\s\S]*?<\/style>/gi, ' ').replace(/<[^>]+>/g, ' ')))
  );

  var rowRx = /([A-Z][a-z]{2}\s+\d{1,2}(?:\s*-\s*(?:[A-Z][a-z]{2}\s+\d{1,2}|\d{1,2})))\s*(\d+)\s*\$([\d,]+(?:\.\d+)?)\s*(-?\d+(?:\.\d+)?%|-)\s*(\d+)\s*(-?\d+|-)\s*\$([\d,]+(?:\.\d+)?)\s*\$([\d,]+(?:\.\d+)?)\s+(\d+)\s+(false|true)\b/ig;

  candidates.forEach(function(candidate) {
    if (!candidate) return;
    var normalized = String(candidate)
      .replace(/[【】]/g, ' ')
      .replace(/\b\d+[†‡]\b/g, ' ')
      .replace(/[†‡]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

    var m;
    rowRx.lastIndex = 0;
    while ((m = rowRx.exec(normalized)) !== null) {
      pushWeekendRow(
        compactWhitespaceV53_(m[1]).replace(/\s*-\s*/g, '-'),
        m[2],
        parseMoney_(m[3]),
        parseMoney_(m[8]),
        parseIntSafe_(m[5]),
        parseMoney_(m[7]),
        m[4],
        m[6],
        m[9],
        m[10],
        [m[1], m[2], '$' + m[3], m[4], m[5], m[6], '$' + m[7], '$' + m[8], m[9], m[10]]
      );
    }
  });

  return dedupRawRecords_(rows);
}

function fetchBoxOfficeMojoTitleCandidate_(titleUrl) {
  var normalized = normalizeBoxOfficeMojoTitleUrl_(titleUrl);
  if (!normalized) throw new Error('Invalid Box Office Mojo title URL');
  var html = fetchUrl_(normalized);
  var text = htmlToText_(html);

  var title = '';
  var year = '';
  var m = compactWhitespaceV53_(text).match(/#\s*(.*?)\s*\((20\d{2})\)/);
  if (m) {
    title = compactWhitespaceV53_(m[1]);
    year = m[2] || '';
  }
  if (!title) {
    var og = html.match(/<meta[^>]+property="og:title"[^>]+content="([^"]+)"/i);
    if (og) {
      var ogt = decodeHtmlEntitiesSimple_(og[1] || '');
      var mOg = ogt.match(/^(.*?)\s*\((20\d{2})\)/);
      if (mOg) {
        title = compactWhitespaceV53_(mOg[1]).trim();
        year = mOg[2] || year;
      }
    }
  }
  if (!title) {
    var h1 = html.match(/<h1[^>]*>\s*([^<]+?)\s*\((20\d{2})\)\s*<\/h1>/i);
    if (h1) {
      title = compactWhitespaceV53_(h1[1]);
      year = h1[2] || year;
    }
  }

  var releaseUrls = extractBoxOfficeMojoReleaseUrlsFromHtmlV541_(html);

  var intlGross = '';
  var ig = compactWhitespaceV53_(text).match(/International\s+\(?100%\)?\s+\$([\d,]+)/i);
  if (ig) intlGross = parseMoney_(ig[1]);

  var areaRows = parseBoxOfficeMojoTitleAreaRows_(html, text, title, year, normalized);
  if (!areaRows.length) {
    var earliest = compactWhitespaceV53_(text).match(/Earliest Release Date\s+([A-Z][a-z]+\s+\d{1,2},\s+20\d{2})\s+\(([^)]+)\)/i);
    if (earliest && intlGross) {
      var cCode = findMarketCodeByCountry_(earliest[2]);
      if (cCode) {
        areaRows = areaRows.concat(parseBoxOfficeMojoTitleAreaRows_('', earliest[2] + ' ' + earliest[1] + ' $' + intlGross, title, year, normalized));
      }
    }
  }

  return {
    title_url: normalized,
    title_en: title,
    release_year: year ? parseInt(year, 10) : '',
    release_urls: releaseUrls,
    intl_gross_usd: intlGross,
    area_rows: dedupRawRecords_(areaRows),
    source_url: normalized
  };
}

function fetchBoxOfficeMojoReleaseEvidence_(releaseUrl, query, yearHint) {
  var url = normalizeBoxOfficeMojoReleaseUrl_(releaseUrl);
  if (!url) throw new Error('Invalid Box Office Mojo release URL');

  var html = fetchUrl_(url);
  var text = htmlToText_(html);
  var rows = [];

  var titleInfo = extractBoxOfficeMojoReleaseTitleV543_(html, text, query);
  var title = titleInfo.title || String(query || '').trim();
  var year = titleInfo.year || String(yearHint || '').slice(0, 4) || '';

  if (!titleMatchesQuery_(query, title, '', year, yearHint)) {
    var hitScore = scoreResolvedTitle_(query, title, '', year, yearHint);
    if (hitScore < 0.55) return [];
  }

  var territory = inferBoxOfficeMojoReleaseCountryV543_(html, text, url);
  var countryCode = territory.code || '';
  var country = territory.country || '';

  var releaseDate = '';
  var rdM = compactWhitespaceV53_(text).match(/Release Date\s*([A-Za-z]{3,9}\s+\d{1,2},\s+20\d{2})/i);
  if (!rdM) {
    rdM = compactWhitespaceV53_(decodeHtmlEntitiesSimple_(html.replace(/<[^>]+>/g, ' '))).match(/Release Date\s*([A-Za-z]{3,9}\s+\d{1,2},\s+20\d{2})/i);
  }
  if (rdM) releaseDate = toIsoDateFromLong_(rdM[1]);
  if (!year && releaseDate) year = String(releaseDate).slice(0, 4);

  var opening = '';
  var openM = compactWhitespaceV53_(text).match(/Opening\s*\$([\d,]+)/i);
  if (!openM) {
    openM = compactWhitespaceV53_(decodeHtmlEntitiesSimple_(html.replace(/<[^>]+>/g, ' '))).match(/Opening\s*\$([\d,]+)/i);
  }
  if (openM) opening = parseMoney_(openM[1]);

  var theaters = '';
  var thM = compactWhitespaceV53_(text).match(/Opening\s*\$[\d,]+\s+(\d+)\s+theaters/i);
  if (!thM) {
    thM = compactWhitespaceV53_(decodeHtmlEntitiesSimple_(html.replace(/<[^>]+>/g, ' '))).match(/Opening\s*\$[\d,]+\s+(\d+)\s+theaters/i);
  }
  if (thM) theaters = parseIntSafe_(thM[1]);

  var blob = compactWhitespaceV53_(text + ' ' + decodeHtmlEntitiesSimple_(html.replace(/<[^>]+>/g, ' ')));
  var gross = '';

  if (country) {
    var countryEsc = escapeRegexV53_(country).replace(/\s+/g, '\\s+');
    var g1 = blob.match(new RegExp(countryEsc + '\\s*\\$([\\d,]+)', 'i'));
    if (g1) gross = parseMoney_(g1[1]);
    if (!gross) {
      var g2 = blob.match(new RegExp('Grosses\\s+' + countryEsc + '\\s*\\$([\\d,]+)', 'i'));
      if (g2) gross = parseMoney_(g2[1]);
    }
    if (!gross) {
      var g3 = blob.match(new RegExp('All Territories\\s+' + countryEsc + '[\\s\\S]{0,200}?\\$([\\d,]+)', 'i'));
      if (g3) gross = parseMoney_(g3[1]);
    }
  }

  if (!gross) {
    var world = blob.match(/Worldwide\s+\$([\d,]+)/i);
    if (world) gross = parseMoney_(world[1]);
  }

  var weekendRows = parseBoxOfficeMojoReleaseWeekendRowsV541_(html, text, {
    title: title,
    year: year || yearHint || '',
    countryCode: countryCode,
    country: country,
    url: url,
    releaseDate: releaseDate,
    gross: gross
  });

  if ((!gross || !opening) && weekendRows && weekendRows.length) {
    var sortedWeekend = weekendRows.slice().sort(function(a, b) {
      return String(a.period_start_date || '').localeCompare(String(b.period_start_date || ''));
    });
    if (!opening) opening = Number(sortedWeekend[0].period_gross_local || '') || opening;
    if (!gross) gross = Number(sortedWeekend[sortedWeekend.length - 1].cumulative_gross_local || '') || gross;
  }

  if (countryCode && (gross || opening)) {
    rows.push(buildRawRecord_({
      source_name: 'BOXOFFICEMOJO_RELEASE',
      source_url: url,
      parser_confidence: 0.93,
      source_entity_id: url + '|lifetime',
      country: country,
      country_code: countryCode,
      film_title_raw: title,
      film_title_ar_raw: '',
      release_year_hint: year || yearHint || '',
      record_scope: 'title',
      record_granularity: 'lifetime',
      record_semantics: 'title_cumulative_total',
      source_confidence: 0.82,
      match_confidence: 0.94,
      evidence_type: 'title_performance',
      period_label_raw: 'lifetime',
      period_start_date: releaseDate || '',
      period_end_date: '',
      period_key: '',
      rank: '',
      period_gross_local: opening || gross || '',
      cumulative_gross_local: gross || opening || '',
      currency: 'USD',
      admissions_actual: '',
      work_id: '',
      distributor: '',
      notes: opening ? ('Opening=' + opening + ' USD; theaters=' + (theaters || '')) : 'Territory cumulative total from release page',
      raw_payload_json: JSON.stringify({ territory: country, gross: gross || '', opening: opening || '', theaters: theaters || '', release_date: releaseDate || '' })
    }));
  }

  if (weekendRows && weekendRows.length) rows = rows.concat(weekendRows);

  return dedupRawRecords_(rows);
}


// ============================================================
// SECTION 15: VISUAL CHART SHEETS + WEEKLY EMAIL AUTOMATION
// Overrides and additions added after v4.1.0
// ============================================================

const VISUAL_SHEETS = {
  ELCINEMA: 'elCinema_Weekly_Charts',
  BOM: 'BOM_Weekly_Charts'
};

const AUTOMATION_CONFIG_DEFAULTS = [
  ['EMAIL_ENABLED', 'no', 'yes/no - send weekly digest emails after scheduled pipeline runs'],
  ['EMAIL_TO', '', 'Recipient email for weekly digest'],
  ['EMAIL_SUBJECT', 'MENA Box Office Weekly Digest', 'Email subject line'],
  ['EMAIL_WEEKDAY', 'MONDAY', 'MONDAY..SUNDAY for the weekly trigger'],
  ['EMAIL_HOUR', '9', 'Local hour for the weekly trigger (0-23)'],
  ['EMAIL_INCLUDE_PIPELINE', 'yes', 'yes/no - run pipeline before sending the digest']
];

function installMenu() {
  SpreadsheetApp.getUi()
    .createMenu('🎬 MENA Intelligence')
    .addItem('Initialize Workbook', 'initializeWorkbook')
    .addItem('Search Title (Fast + Queue)', 'searchTitleFromSheet')
    .addItem('Refresh Live Lookup (Fast + Queue)', 'refreshLiveLookupFromSheet')
    .addSeparator()
    .addItem('Open Dashboard', 'openDashboard')
    .addItem('Update Dashboard', 'updateDashboardFromSheet')
    .addItem('Refresh Weekly Charts', 'refreshWeeklyChartsSheets')
    .addSeparator()
    .addItem('Process Lookup Queue', 'processLookupQueue')
    .addItem('Install Lookup Worker Trigger', 'installLookupWorkerTrigger')
    .addItem('Remove Lookup Worker Trigger', 'removeLookupWorkerTrigger')
    .addSeparator()
    .addItem('Install Weekly Pipeline + Email Trigger', 'installWeeklyPipelineEmailTrigger')
    .addItem('Remove Weekly Pipeline + Email Trigger', 'removeWeeklyPipelineEmailTrigger')
    .addItem('Send Weekly Email Now', 'sendWeeklyDigestNow')
    .addSeparator()
    .addItem('Run Pipeline', 'runPipeline')
    .addItem('Review Queue', 'openReviewQueue')
    .addItem('Test Sources', 'testSources')
    .addToUi();
}

function onOpen() {
  installMenu();
}

function initializeWorkbook() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  ensureLookupJobsSheet_(ss);

  var lookupSheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  var dashboardSheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  var existingLookupQuery = lookupSheet ? String(lookupSheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '') : '';
  var existingDashboardQuery = dashboardSheet ? String(dashboardSheet.getRange('B2').getValue() || '') : '';
  var existingDashboardCountry = dashboardSheet ? String(dashboardSheet.getRange('B3').getValue() || 'ALL') : 'ALL';

  seedSystemConfig_(ss);
  seedAutomationConfig_(ss);
  seedTicketPrices_(ss);
  setupLookupSheet_(ss, existingLookupQuery);
  setupDashboardSheet_(ss, existingDashboardQuery || existingLookupQuery, existingDashboardCountry);
  setupVisualChartsSheet_(ss, VISUAL_SHEETS.ELCINEMA, 'elCinema Weekly Charts');
  setupVisualChartsSheet_(ss, VISUAL_SHEETS.BOM, 'Box Office Mojo Weekly Charts');
  writeLookupJobBanner_(ss, null, 'Idle');
  refreshWeeklyChartsSheets_();
  SpreadsheetApp.getActive().toast('Workbook initialized with dashboard, visual weekly charts, and email settings.', 'MENA Intelligence', 6);
}

function ensureAllSheets_(ss) {
  const ordered = [
    [CONFIG.SHEETS.LOOKUP, SCHEMAS.CONFIG],
    [CONFIG.SHEETS.DASHBOARD, SCHEMAS.CONFIG],
    [CONFIG.SHEETS.RAW, SCHEMAS.RAW],
    [CONFIG.SHEETS.RECON, SCHEMAS.RECON],
    [CONFIG.SHEETS.FILMS, SCHEMAS.FILMS],
    [CONFIG.SHEETS.ALIASES, SCHEMAS.ALIASES],
    [CONFIG.SHEETS.REVIEW, SCHEMAS.REVIEW],
    [CONFIG.SHEETS.RUN_LOG, SCHEMAS.RUN_LOG],
    [CONFIG.SHEETS.TICKET_PRICES, SCHEMAS.TICKET_PRICES],
    [CONFIG.SHEETS.SOURCE_STATUS, SCHEMAS.SOURCE_STATUS],
    [CONFIG.SHEETS.CONFIG, SCHEMAS.CONFIG]
  ];

  ordered.forEach(function(item) {
    const name = item[0];
    const schema = item[1];
    let sheet = ss.getSheetByName(name);
    if (!sheet) sheet = ss.insertSheet(name);
    if (name === CONFIG.SHEETS.LOOKUP || name === CONFIG.SHEETS.DASHBOARD) return;
    ensureHeader_(sheet, schema);
  });

  setupLookupSheet_(ss);
  setupDashboardSheet_(ss);
  setupVisualChartsSheet_(ss, VISUAL_SHEETS.ELCINEMA, 'elCinema Weekly Charts');
  setupVisualChartsSheet_(ss, VISUAL_SHEETS.BOM, 'Box Office Mojo Weekly Charts');
  cleanupDefaultSheets_(ss);
}

function setupDashboardSheet_(ss, preserveQuery, preserveCountry) {
  var sheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  if (!sheet) sheet = ss.insertSheet(CONFIG.SHEETS.DASHBOARD);

  var existingQuery = preserveQuery !== undefined ? preserveQuery : String(sheet.getRange('B2').getValue() || '');
  var existingCountry = preserveCountry !== undefined ? preserveCountry : String(sheet.getRange('B3').getValue() || 'ALL');

  sheet.clear();
  try { sheet.clearCharts(); } catch (e) {}

  sheet.getRange('A1').setValue('MENA Box Office Performance Dashboard').setFontSize(15).setFontWeight('bold');
  sheet.getRange('A2').setValue('Title Query').setFontWeight('bold');
  sheet.getRange('B2').setValue(existingQuery).setBackground('#FFF9C4').setFontWeight('bold');
  sheet.getRange('A3').setValue('Country Filter').setFontWeight('bold');
  sheet.getRange('B3').setValue(existingCountry || 'ALL').setBackground('#E0F2FE').setFontWeight('bold');
  var validation = SpreadsheetApp.newDataValidation().requireValueInList(['ALL'].concat(Object.keys(CONFIG.MARKETS)), true).setAllowInvalid(false).build();
  sheet.getRange('B3').setDataValidation(validation);
  sheet.getRange('A4').setValue('Use this dashboard to inspect one selected title clearly, without duplicate helper columns.').setFontColor('#6B7280');
  sheet.setFrozenRows(4);
  sheet.setColumnWidths(1, 12, 150);
  sheet.setColumnWidth(2, 280);
}

function updateDashboardFromSheet() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  var sheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  var query = String(sheet.getRange('B2').getValue() || '').trim();
  var countryCode = String(sheet.getRange('B3').getValue() || 'ALL').trim().toUpperCase();
  if (!query) {
    SpreadsheetApp.getUi().alert('Enter a title in B2 on the dashboard sheet.');
    return;
  }
  renderDashboard_(query, countryCode || 'ALL');
  refreshWeeklyChartsSheets_();
}

function renderDashboard_(query, countryCode) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  var sheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  setupDashboardSheet_(ss, query, countryCode);
  clearDashboardOutput_(sheet);

  var recon = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RECON));
  var films = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.FILMS));
  var aliases = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.ALIASES));
  var match = matchQueryToFilm_(query, films, aliases);

  var relevant = [];
  var normQueryEn = normalizeLatinTitle_(query);
  var normQueryAr = normalizeArabicTitle_(query);

  if (match.film_id) {
    relevant = recon.filter(function(r) { return r.film_id === match.film_id; });
  } else {
    relevant = recon.filter(function(r) {
      var t1 = normalizeLatinTitle_(r.canonical_title || r.film_title_raw || '');
      var t2 = normalizeArabicTitle_(r.canonical_title_ar || r.film_title_ar_raw || '');
      return (t1 && (t1.indexOf(normQueryEn) >= 0 || normQueryEn.indexOf(t1) >= 0 || titleSimilarity_(normQueryEn, t1) >= 0.75)) ||
             (t2 && (t2.indexOf(normQueryAr) >= 0 || normQueryAr.indexOf(t2) >= 0 || titleSimilarity_(normQueryAr, t2) >= 0.80));
    });
  }

  var comparable = relevant.filter(function(r) {
    return r.record_scope === 'title' && r.record_semantics === 'title_period_gross';
  }).slice();

  comparable.sort(compareDashboardPeriods_);

  var coverageCounts = {};
  comparable.forEach(function(r) { coverageCounts[r.country_code] = (coverageCounts[r.country_code] || 0) + 1; });

  var chosenCountry = countryCode && countryCode !== 'ALL' ? countryCode : '';
  if (!chosenCountry) {
    var bestCode = '';
    var bestCount = -1;
    Object.keys(coverageCounts).forEach(function(code) {
      if (coverageCounts[code] > bestCount) {
        bestCode = code;
        bestCount = coverageCounts[code];
      }
    });
    chosenCountry = bestCode || 'ALL';
  }

  sheet.getRange('B2').setValue(query);
  sheet.getRange('B3').setValue(chosenCountry || 'ALL');

  var filtered = chosenCountry === 'ALL' ? comparable : comparable.filter(function(r) { return r.country_code === chosenCountry; });
  var chartRows = filtered.slice().sort(compareDashboardPeriods_).map(function(r, idx, arr) {
    return {
      display_period: dashboardPeriodLabel_(r, idx, arr),
      period_start_date: normalizeDashboardDateValue_(r.period_start_date),
      period_end_date: normalizeDashboardDateValue_(r.period_end_date),
      period_key: r.period_key || r.period_label_raw || '',
      country: r.country || '',
      country_code: r.country_code || '',
      record_granularity: r.record_granularity || '',
      gross: Number(r.period_gross_local || 0),
      currency: r.currency || '',
      source_name: r.source_name || ''
    };
  });

  var totalGross = chartRows.reduce(function(sum, r) { return sum + (Number(r.gross) || 0); }, 0);
  var peakRow = chartRows.slice().sort(function(a, b) { return Number(b.gross || 0) - Number(a.gross || 0); })[0] || null;
  var latestRow = chartRows.length ? chartRows[chartRows.length - 1] : null;

  var summary = [
    ['Matched Film', match.title || query, 'Match Confidence', match.score || ''],
    ['Country Shown', chosenCountry === 'ALL' ? 'All comparable markets' : ((CONFIG.MARKETS[chosenCountry] && CONFIG.MARKETS[chosenCountry].country) || chosenCountry), 'Records', chartRows.length],
    ['Total Period Gross', totalGross || '', 'Currency', chartRows.length ? unique_(chartRows.map(function(r) { return r.currency; }).filter(Boolean)).join(', ') : ''],
    ['Peak Period', peakRow ? peakRow.display_period : '', 'Peak Gross', peakRow ? peakRow.gross : ''],
    ['Latest Period', latestRow ? latestRow.display_period : '', 'Latest Gross', latestRow ? latestRow.gross : '']
  ];
  sheet.getRange(6, 1, summary.length, 4).setValues(summary);
  sheet.getRange(6, 1, 1, 4).setFontWeight('bold').setBackground('#E5E7EB');

  var noteRow = 12;
  var note = !comparable.length
    ? 'No title-level performance rows are available for this title yet.'
    : (chosenCountry === 'ALL'
      ? 'Choose one country in B3 to see a clean single-market trend chart.'
      : 'Rows are sorted by true period dates. Inverted weekend markets are labeled as Weekend XX.');
  sheet.getRange(noteRow, 1).setValue('Dashboard Notes').setFontWeight('bold').setBackground('#FDE68A');
  sheet.getRange(noteRow + 1, 1).setValue(note);

  var marketSummaryRow = noteRow + 3;
  sheet.getRange(marketSummaryRow, 1).setValue('Available Market Coverage').setFontWeight('bold').setBackground('#DBEAFE');
  var marketHeader = [['Country', 'Code', 'Records', 'First Period', 'Last Period']];
  sheet.getRange(marketSummaryRow + 1, 1, 1, marketHeader[0].length).setValues(marketHeader).setFontWeight('bold');
  var marketTable = Object.keys(coverageCounts).sort().map(function(code) {
    var rows = comparable.filter(function(r) { return r.country_code === code; }).sort(compareDashboardPeriods_);
    return [
      (CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code,
      code,
      rows.length,
      rows.length ? dashboardPeriodLabel_(rows[0], 0, rows) : '',
      rows.length ? dashboardPeriodLabel_(rows[rows.length - 1], rows.length - 1, rows) : ''
    ];
  });
  if (marketTable.length) sheet.getRange(marketSummaryRow + 2, 1, marketTable.length, marketHeader[0].length).setValues(marketTable);
  else sheet.getRange(marketSummaryRow + 2, 1).setValue('No market coverage found.');

  var tableRow = marketSummaryRow + 4 + Math.max(marketTable.length, 1);
  sheet.getRange(tableRow, 1).setValue('Performance Table').setFontWeight('bold').setBackground('#D1FAE5');
  var header = [['Period', 'Start Date', 'End Date', 'Country', 'Period Gross (Local)', 'Currency', 'Source']];
  sheet.getRange(tableRow + 1, 1, 1, header[0].length).setValues(header).setFontWeight('bold');
  var tableValues = chartRows.map(function(r) {
    return [r.display_period, r.period_start_date, r.period_end_date, r.country, r.gross, r.currency, r.source_name];
  });
  if (tableValues.length) {
    sheet.getRange(tableRow + 2, 1, tableValues.length, header[0].length).setValues(tableValues);
    sheet.getRange(tableRow + 2, 2, tableValues.length, 2).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(tableRow + 2, 5, tableValues.length, 1).setNumberFormat('#,##0.00');
  } else {
    sheet.getRange(tableRow + 2, 1).setValue('No rows for the selected title / market.');
  }

  try { sheet.clearCharts(); } catch (e) {}
  if (tableValues.length && chosenCountry !== 'ALL') {
    var helperCol = 13;
    var chartDataHeader = [['Period', 'Period Gross']];
    var chartDataValues = chartRows.map(function(r) { return [r.display_period, r.gross]; });
    sheet.getRange(tableRow + 1, helperCol, 1, 2).setValues(chartDataHeader).setFontWeight('bold');
    sheet.getRange(tableRow + 2, helperCol, chartDataValues.length, 2).setValues(chartDataValues);
    var chartBuilder = sheet.newChart()
      .asLineChart()
      .addRange(sheet.getRange(tableRow + 1, helperCol, chartDataValues.length + 1, 2))
      .setPosition(6, 6, 0, 0)
      .setOption('title', 'Performance Trend')
      .setOption('legend', { position: 'none' })
      .setOption('hAxis', { title: 'Period' })
      .setOption('vAxis', { title: 'Gross (' + (chartRows[0].currency || 'Local') + ')' });
    sheet.insertChart(chartBuilder.build());
    try { sheet.hideColumns(helperCol, 2); } catch (e2) {}
  }
  autoResize_(sheet, 12);
}

function refreshWeeklyChartsSheets() {
  refreshWeeklyChartsSheets_();
  SpreadsheetApp.getActive().toast('Weekly chart sheets refreshed.', 'MENA Intelligence', 5);
}

function refreshWeeklyChartsSheets_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  var recon = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RECON));

  renderWeeklyChartsSheet_(ss, VISUAL_SHEETS.ELCINEMA, {
    title: 'elCinema Weekly Charts',
    sourceFilter: function(r) {
      return String(r.source_name || '') === 'ELCINEMA_TITLE_BOXOFFICE';
    }
  }, recon);

  renderWeeklyChartsSheet_(ss, VISUAL_SHEETS.BOM, {
    title: 'Box Office Mojo Weekly Charts',
    sourceFilter: function(r) {
      return /^BOXOFFICEMOJO/i.test(String(r.source_name || ''));
    }
  }, recon);
}

function setupVisualChartsSheet_(ss, sheetName, title) {
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) sheet = ss.insertSheet(sheetName);
  if (!sheet.getRange('A1').getValue()) {
    sheet.clear();
    sheet.getRange('A1').setValue(title).setFontSize(15).setFontWeight('bold');
    sheet.getRange('A2').setValue('Friendly visual market charts by period and country. Each country shows a visible matrix plus a chart for the strongest titles.');
    sheet.getRange('A3').setValue('Top Titles Per Country').setFontWeight('bold');
    sheet.getRange('B3').setValue(8).setBackground('#FFF9C4').setFontWeight('bold');
    sheet.getRange('A4').setValue('Updated At').setFontWeight('bold');
    sheet.setFrozenRows(4);
    sheet.setColumnWidths(1, 16, 130);
    sheet.setColumnWidth(1, 180);
  }
  return sheet;
}

function renderWeeklyChartsSheet_(ss, sheetName, options, recon) {
  var sheet = setupVisualChartsSheet_(ss, sheetName, options.title);
  var topN = Number(sheet.getRange('B3').getValue() || 8);
  if (!topN || topN < 1) topN = 8;

  var keepTopRows = 4;
  if (sheet.getMaxRows() > keepTopRows) {
    sheet.getRange(keepTopRows + 1, 1, sheet.getMaxRows() - keepTopRows, Math.max(sheet.getMaxColumns(), 1)).clearContent().clearFormat();
  }
  try { sheet.clearCharts(); } catch (e) {}
  sheet.getRange('B4').setValue(isoNow_());

  var rows = (recon || []).filter(function(r) {
    return r.record_scope === 'title' &&
      r.record_semantics === 'title_period_gross' &&
      !!r.country_code &&
      options.sourceFilter(r);
  }).slice();

  if (!rows.length) {
    sheet.getRange(6, 1).setValue('No comparable weekly/title rows found for this source group yet.');
    return;
  }

  var byCountry = {};
  rows.forEach(function(r) {
    var code = String(r.country_code || '');
    if (!byCountry[code]) byCountry[code] = [];
    byCountry[code].push(r);
  });

  var countryCodes = Object.keys(byCountry).sort();
  var rowPtr = 6;

  countryCodes.forEach(function(code) {
    var countryRows = byCountry[code].slice().sort(compareDashboardPeriods_);
    var countryName = (CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code;

    var totalsByTitle = {};
    countryRows.forEach(function(r) {
      var title = String(r.canonical_title || r.film_title_raw || 'Untitled');
      if (!totalsByTitle[title]) totalsByTitle[title] = 0;
      totalsByTitle[title] += Number(r.period_gross_local || 0);
    });

    var topTitles = Object.keys(totalsByTitle).sort(function(a, b) {
      return totalsByTitle[b] - totalsByTitle[a];
    }).slice(0, topN);

    var byPeriod = {};
    countryRows.forEach(function(r) {
      var label = dashboardPeriodLabel_(r, 0, []);
      var sortVal = dashboardSortValue_(r.period_start_date, r.period_key);
      if (!byPeriod[label]) byPeriod[label] = { sortVal: sortVal, values: {} };
      var title = String(r.canonical_title || r.film_title_raw || 'Untitled');
      if (topTitles.indexOf(title) >= 0) {
        byPeriod[label].values[title] = Number(r.period_gross_local || 0);
      }
    });

    var periodLabels = Object.keys(byPeriod).sort(function(a, b) {
      return byPeriod[a].sortVal - byPeriod[b].sortVal || a.localeCompare(b);
    });

    sheet.getRange(rowPtr, 1).setValue(countryName + ' (' + code + ')').setFontWeight('bold').setBackground('#DBEAFE');
    sheet.getRange(rowPtr + 1, 1).setValue('Visible chart for top ' + topTitles.length + ' titles in this market. Periods are correctly sorted.');
    var header = ['Period'].concat(topTitles);
    sheet.getRange(rowPtr + 2, 1, 1, header.length).setValues([header]).setFontWeight('bold').setBackground('#E5E7EB');

    var matrix = periodLabels.map(function(label) {
      return [label].concat(topTitles.map(function(title) {
        return byPeriod[label].values[title] || 0;
      }));
    });

    if (matrix.length) {
      sheet.getRange(rowPtr + 3, 1, matrix.length, header.length).setValues(matrix);
      if (header.length > 1) sheet.getRange(rowPtr + 3, 2, matrix.length, header.length - 1).setNumberFormat('#,##0.00');
    } else {
      sheet.getRange(rowPtr + 3, 1).setValue('No chartable rows found.');
    }

    if (matrix.length && topTitles.length) {
      var chartRange = sheet.getRange(rowPtr + 2, 1, matrix.length + 1, header.length);
      var chart = sheet.newChart()
        .asColumnChart()
        .addRange(chartRange)
        .setPosition(rowPtr, Math.min(8, Math.max(3, header.length + 2)), 0, 0)
        .setOption('title', countryName + ' Weekly / Weekend Performance')
        .setOption('legend', { position: 'right' })
        .setOption('hAxis', { title: 'Period', slantedText: true, slantedTextAngle: 45 })
        .setOption('vAxis', { title: 'Gross (' + unique_(countryRows.map(function(r) { return r.currency; }).filter(Boolean)).join(', ') + ')' })
        .setOption('isStacked', false);
      sheet.insertChart(chart.build());
    }

    rowPtr += Math.max(matrix.length, 8) + 8;
  });

  autoResize_(sheet, Math.min(sheet.getMaxColumns(), 16));
}

function seedAutomationConfig_(ss) {
  var sheet = ss.getSheetByName(CONFIG.SHEETS.CONFIG);
  if (!sheet) return;
  ensureHeader_(sheet, SCHEMAS.CONFIG);
  var rows = sheet.getLastRow() > 1 ? sheet.getRange(2, 1, sheet.getLastRow() - 1, 3).getValues() : [];
  var existing = {};
  rows.forEach(function(r) { existing[String(r[0] || '')] = true; });
  var toAdd = AUTOMATION_CONFIG_DEFAULTS.filter(function(r) { return !existing[r[0]]; });
  if (toAdd.length) {
    sheet.getRange(sheet.getLastRow() + 1, 1, toAdd.length, 3).setValues(toAdd);
  }
}

function getSystemConfigMap_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(CONFIG.SHEETS.CONFIG);
  var map = {};
  if (!sheet || sheet.getLastRow() < 2) return map;
  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, 2).getValues();
  values.forEach(function(r) {
    map[String(r[0] || '').trim()] = r[1];
  });
  return map;
}

function installWeeklyPipelineEmailTrigger() {
  removeWeeklyPipelineEmailTrigger();
  seedAutomationConfig_(SpreadsheetApp.getActiveSpreadsheet());
  var cfg = getSystemConfigMap_();
  var weekday = normalizeWeekdayName_(cfg.EMAIL_WEEKDAY || 'MONDAY');
  var hour = Number(cfg.EMAIL_HOUR || 9);
  if (isNaN(hour) || hour < 0 || hour > 23) hour = 9;

  ScriptApp.newTrigger('weeklyPipelineAndEmail')
    .timeBased()
    .onWeekDay(ScriptApp.WeekDay[weekday])
    .atHour(hour)
    .create();

  SpreadsheetApp.getActive().toast('Weekly pipeline + email trigger installed for ' + weekday + ' at ' + hour + ':00.', 'MENA Intelligence', 7);
}

function removeWeeklyPipelineEmailTrigger() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'weeklyPipelineAndEmail') ScriptApp.deleteTrigger(t);
  });
}

function normalizeWeekdayName_(value) {
  var s = String(value || 'MONDAY').trim().toUpperCase();
  var allowed = ['SUNDAY','MONDAY','TUESDAY','WEDNESDAY','THURSDAY','FRIDAY','SATURDAY'];
  return allowed.indexOf(s) >= 0 ? s : 'MONDAY';
}

function weeklyPipelineAndEmail() {
  var cfg = getSystemConfigMap_();
  var shouldRunPipeline = String(cfg.EMAIL_INCLUDE_PIPELINE || 'yes').toLowerCase() === 'yes';
  if (shouldRunPipeline) {
    try {
      runPipeline();
    } catch (e) {
      // In trigger context, UI alerts can throw after the core work finishes.
      Logger.log('weeklyPipelineAndEmail runPipeline warning: ' + e.message);
    }
  }
  try {
    refreshWeeklyChartsSheets_();
  } catch (e2) {
    Logger.log('weeklyPipelineAndEmail refreshWeeklyCharts warning: ' + e2.message);
  }
  sendWeeklyDigestEmail_();
}

function sendWeeklyDigestNow() {
  seedAutomationConfig_(SpreadsheetApp.getActiveSpreadsheet());
  var sent = sendWeeklyDigestEmail_();
  SpreadsheetApp.getActive().toast(sent ? 'Weekly digest email sent.' : 'Weekly digest email skipped. Check System_Config values.', 'MENA Intelligence', 6);
}

function sendWeeklyDigestEmail_() {
  var cfg = getSystemConfigMap_();
  var enabled = String(cfg.EMAIL_ENABLED || 'no').toLowerCase() === 'yes';
  var to = String(cfg.EMAIL_TO || '').trim();
  if (!enabled || !to) return false;

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var runSheet = ss.getSheetByName(CONFIG.SHEETS.RUN_LOG);
  var reconSheet = ss.getSheetByName(CONFIG.SHEETS.RECON);
  var sourceStatusSheet = ss.getSheetByName(CONFIG.SHEETS.SOURCE_STATUS);

  var latestRun = null;
  if (runSheet && runSheet.getLastRow() >= 2) {
    latestRun = readSheetObjects_(runSheet).slice().sort(function(a, b) {
      return String(b.started_at || '').localeCompare(String(a.started_at || ''));
    })[0] || null;
  }

  var reconRows = reconSheet ? readSheetObjects_(reconSheet) : [];
  var weeklyRows = reconRows.filter(function(r) { return r.record_scope === 'title' && r.record_semantics === 'title_period_gross'; });
  var films = unique_(weeklyRows.map(function(r) { return r.film_id || r.canonical_title; }).filter(Boolean)).length;
  var markets = unique_(weeklyRows.map(function(r) { return r.country_code; }).filter(Boolean)).join(', ');

  var statuses = sourceStatusSheet ? readSheetObjects_(sourceStatusSheet).slice(-10) : [];
  var statusLines = statuses.map(function(s) {
    return '- ' + (s.source_name || '') + ' [' + (s.country || 'All') + ']: ' + (s.status || '') + ' (' + (s.rows || 0) + ' rows)';
  }).join('\n');

  var body = [
    'MENA Box Office Weekly Digest',
    '',
    'Spreadsheet: ' + ss.getName(),
    'Generated: ' + isoNow_(),
    '',
    latestRun ? ('Latest Run ID: ' + (latestRun.run_id || '')) : 'Latest Run ID: n/a',
    latestRun ? ('Started: ' + (latestRun.started_at || '')) : '',
    latestRun ? ('Completed: ' + (latestRun.completed_at || '')) : '',
    latestRun ? ('Rows Fetched: ' + (latestRun.rows_fetched || 0)) : '',
    latestRun ? ('Raw Added: ' + (latestRun.raw_added || 0)) : '',
    latestRun ? ('Reconciled Written: ' + (latestRun.reconciled_written || 0)) : '',
    '',
    'Current title-level weekly records: ' + weeklyRows.length,
    'Tracked titles with weekly performance: ' + films,
    'Markets covered: ' + (markets || 'None'),
    '',
    'Recent source checks:',
    statusLines || '- No recent source checks found.',
    '',
    'Workbook URL:',
    ss.getUrl()
  ].filter(function(x) { return x !== ''; }).join('\n');

  MailApp.sendEmail({
    to: to,
    subject: String(cfg.EMAIL_SUBJECT || 'MENA Box Office Weekly Digest'),
    body: body
  });
  return true;
}


// ============================================================
// SECTION 16: CONFIG-FIRST WEEKLY EMAIL OVERRIDES
// This block intentionally comes last so it overrides earlier definitions
// without changing the core scraping / reconciliation engine.
// ============================================================

const USER_DEFAULTS = {
  EMAIL_ENABLED: 'yes',
  EMAIL_TO: 'replace-with-your-email@example.com',
  EMAIL_MODE: 'BOTH', // DIGEST | ALERT | BOTH
  EMAIL_SUBJECT: 'MENA Box Office Weekly Update',
  EMAIL_WEEKDAY: 'MONDAY',
  EMAIL_HOUR: '9',
  EMAIL_INCLUDE_PIPELINE: 'yes',
  EMAIL_SEND_IF_EMPTY: 'no',
  ALERT_THRESHOLD_USD: '500000',
  ALERT_MAX_TITLES: '12'
};

function installMenu() {
  SpreadsheetApp.getUi()
    .createMenu('🎬 MENA Intelligence')
    .addItem('Initialize Workbook', 'initializeWorkbook')
    .addSeparator()
    .addItem('Run Pipeline', 'runPipeline')
    .addItem('Run Weekly Pipeline + Email Now', 'runWeeklyPipelineEmailNow')
    .addItem('Send Weekly Email Now', 'sendWeeklyDigestNow')
    .addSeparator()
    .addItem('Search Title (Fast + Queue)', 'searchTitleFromSheet')
    .addItem('Refresh Live Lookup (Fast + Queue)', 'refreshLiveLookupFromSheet')
    .addSeparator()
    .addItem('Open Dashboard', 'openDashboard')
    .addItem('Update Dashboard', 'updateDashboardFromSheet')
    .addItem('Refresh Weekly Charts', 'refreshWeeklyChartsSheets')
    .addSeparator()
    .addItem('Process Lookup Queue', 'processLookupQueue')
    .addItem('Install Lookup Worker Trigger', 'installLookupWorkerTrigger')
    .addItem('Remove Lookup Worker Trigger', 'removeLookupWorkerTrigger')
    .addSeparator()
    .addItem('Install Weekly Pipeline + Email Trigger', 'installWeeklyPipelineEmailTrigger')
    .addItem('Remove Weekly Pipeline + Email Trigger', 'removeWeeklyPipelineEmailTrigger')
    .addToUi();
}

function onOpen() {
  installMenu();
}

function initializeWorkbook() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  ensureLookupJobsSheet_(ss);

  var lookupSheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  var dashboardSheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  var existingLookupQuery = lookupSheet ? String(lookupSheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '') : '';
  var existingDashboardQuery = dashboardSheet ? String(dashboardSheet.getRange('B2').getValue() || '') : '';
  var existingDashboardCountry = dashboardSheet ? String(dashboardSheet.getRange('B3').getValue() || 'ALL') : 'ALL';

  seedSystemConfig_(ss);
  seedTicketPrices_(ss);
  setupLookupSheet_(ss, existingLookupQuery);
  setupDashboardSheet_(ss, existingDashboardQuery || existingLookupQuery, existingDashboardCountry);
  setupVisualChartsSheet_(ss, VISUAL_SHEETS.ELCINEMA, 'elCinema Weekly Charts');
  setupVisualChartsSheet_(ss, VISUAL_SHEETS.BOM, 'Box Office Mojo Weekly Charts');
  writeLookupJobBanner_(ss, null, 'Idle');
  refreshWeeklyChartsSheets_();
  SpreadsheetApp.getActive().toast('Workbook initialized. Start with System_Config, then install the weekly trigger.', 'MENA Intelligence', 7);
}

function seedSystemConfig_(ss) {
  var sheet = ss.getSheetByName(CONFIG.SHEETS.CONFIG);
  if (!sheet) return;
  ensureHeader_(sheet, SCHEMAS.CONFIG);

  var existing = {};
  if (sheet.getLastRow() >= 2) {
    sheet.getRange(2, 1, sheet.getLastRow() - 1, 3).getValues().forEach(function(r) {
      var key = String(r[0] || '').trim();
      if (key) existing[key] = { value: r[1], notes: r[2] };
    });
  }

  var orderedRows = [
    ['EMAIL_ENABLED', existing.EMAIL_ENABLED ? existing.EMAIL_ENABLED.value : USER_DEFAULTS.EMAIL_ENABLED, 'yes/no - master switch for weekly email sending'],
    ['EMAIL_TO', existing.EMAIL_TO ? existing.EMAIL_TO.value : USER_DEFAULTS.EMAIL_TO, 'Recipient email address'],
    ['EMAIL_MODE', existing.EMAIL_MODE ? existing.EMAIL_MODE.value : USER_DEFAULTS.EMAIL_MODE, 'DIGEST | ALERT | BOTH'],
    ['EMAIL_SUBJECT', existing.EMAIL_SUBJECT ? existing.EMAIL_SUBJECT.value : USER_DEFAULTS.EMAIL_SUBJECT, 'Email subject line'],
    ['EMAIL_WEEKDAY', existing.EMAIL_WEEKDAY ? existing.EMAIL_WEEKDAY.value : USER_DEFAULTS.EMAIL_WEEKDAY, 'MONDAY..SUNDAY for weekly trigger'],
    ['EMAIL_HOUR', existing.EMAIL_HOUR ? existing.EMAIL_HOUR.value : USER_DEFAULTS.EMAIL_HOUR, 'Local script hour for weekly trigger (0-23)'],
    ['EMAIL_INCLUDE_PIPELINE', existing.EMAIL_INCLUDE_PIPELINE ? existing.EMAIL_INCLUDE_PIPELINE.value : USER_DEFAULTS.EMAIL_INCLUDE_PIPELINE, 'yes/no - run pipeline before charts/email'],
    ['EMAIL_SEND_IF_EMPTY', existing.EMAIL_SEND_IF_EMPTY ? existing.EMAIL_SEND_IF_EMPTY.value : USER_DEFAULTS.EMAIL_SEND_IF_EMPTY, 'yes/no - still send email when there are no alerts or coverage'],
    ['ALERT_THRESHOLD_USD', existing.ALERT_THRESHOLD_USD ? existing.ALERT_THRESHOLD_USD.value : USER_DEFAULTS.ALERT_THRESHOLD_USD, 'USD threshold used for alert mode'],
    ['ALERT_MAX_TITLES', existing.ALERT_MAX_TITLES ? existing.ALERT_MAX_TITLES.value : USER_DEFAULTS.ALERT_MAX_TITLES, 'Maximum number of alert titles listed'],
    ['version', CONFIG.VERSION, 'Workbook version'],
    ['timezone', CONFIG.TIMEZONE, 'Script timezone used by this project'],
    ['freshness_rule', 'Freshness is source-specific and not equivalent across source types', 'Informational'],
    ['ticket_estimate_rule', 'Only shown when ticket-price confidence is high and evidence is title-performance', 'Informational'],
    ['testing_tip', 'Use menu item: Run Weekly Pipeline + Email Now', 'Quick end-to-end test entry point']
  ];

  rewriteSheetObjects_(sheet, SCHEMAS.CONFIG, orderedRows.map(function(r) {
    return { key: r[0], value: r[1], notes: r[2] };
  }));
}

function seedAutomationConfig_(ss) {
  seedSystemConfig_(ss);
}

function installWeeklyPipelineEmailTrigger() {
  removeWeeklyPipelineEmailTrigger();
  seedSystemConfig_(SpreadsheetApp.getActiveSpreadsheet());
  var cfg = getSystemConfigMap_();
  var weekday = normalizeWeekdayName_(cfg.EMAIL_WEEKDAY || USER_DEFAULTS.EMAIL_WEEKDAY);
  var hour = Number(cfg.EMAIL_HOUR || USER_DEFAULTS.EMAIL_HOUR);
  if (isNaN(hour) || hour < 0 || hour > 23) hour = Number(USER_DEFAULTS.EMAIL_HOUR);

  ScriptApp.newTrigger('weeklyPipelineAndEmail')
    .timeBased()
    .onWeekDay(ScriptApp.WeekDay[weekday])
    .atHour(hour)
    .create();

  SpreadsheetApp.getActive().toast('Weekly pipeline + email trigger installed for ' + weekday + ' at ' + hour + ':00.', 'MENA Intelligence', 7);
}

function runWeeklyPipelineEmailNow() {
  var sent = weeklyPipelineAndEmail();
  SpreadsheetApp.getActive().toast(sent ? 'Weekly flow completed and email sent.' : 'Weekly flow completed. Email skipped by config.', 'MENA Intelligence', 7);
}

function formatNumberForEmail_(value) {
  var num = Number(value || 0);
  if (!isFinite(num)) return String(value || '');
  var rounded = Math.round(num * 100) / 100;
  return rounded.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function weeklyPipelineAndEmail() {
  var cfg = getSystemConfigMap_();
  var shouldRunPipeline = String(cfg.EMAIL_INCLUDE_PIPELINE || USER_DEFAULTS.EMAIL_INCLUDE_PIPELINE).toLowerCase() === 'yes';

  if (shouldRunPipeline) {
    try {
      runPipeline();
    } catch (e) {
      Logger.log('weeklyPipelineAndEmail runPipeline warning: ' + e.message);
    }
  }

  try {
    refreshWeeklyChartsSheets_();
  } catch (e2) {
    Logger.log('weeklyPipelineAndEmail refreshWeeklyCharts warning: ' + e2.message);
  }

  try {
    updateDashboardFromSheet();
  } catch (e3) {
    Logger.log('weeklyPipelineAndEmail updateDashboard warning: ' + e3.message);
  }

  return sendWeeklyDigestEmail_();
}

function sendWeeklyDigestNow() {
  seedSystemConfig_(SpreadsheetApp.getActiveSpreadsheet());
  var sent = sendWeeklyDigestEmail_();
  SpreadsheetApp.getActive().toast(sent ? 'Weekly email sent.' : 'Weekly email skipped. Check System_Config values.', 'MENA Intelligence', 6);
}

function sendWeeklyDigestEmail_() {
  var cfg = getSystemConfigMap_();
  var enabled = String(cfg.EMAIL_ENABLED || USER_DEFAULTS.EMAIL_ENABLED).toLowerCase() === 'yes';
  var to = String(cfg.EMAIL_TO || USER_DEFAULTS.EMAIL_TO).trim();
  if (!enabled || !to || to === USER_DEFAULTS.EMAIL_TO) return false;

  var mode = String(cfg.EMAIL_MODE || USER_DEFAULTS.EMAIL_MODE).trim().toUpperCase();
  if (['DIGEST', 'ALERT', 'BOTH'].indexOf(mode) < 0) mode = USER_DEFAULTS.EMAIL_MODE;
  var sendIfEmpty = String(cfg.EMAIL_SEND_IF_EMPTY || USER_DEFAULTS.EMAIL_SEND_IF_EMPTY).toLowerCase() === 'yes';
  var threshold = Number(cfg.ALERT_THRESHOLD_USD || USER_DEFAULTS.ALERT_THRESHOLD_USD);
  if (isNaN(threshold) || threshold < 0) threshold = Number(USER_DEFAULTS.ALERT_THRESHOLD_USD);
  var alertMaxTitles = Number(cfg.ALERT_MAX_TITLES || USER_DEFAULTS.ALERT_MAX_TITLES);
  if (isNaN(alertMaxTitles) || alertMaxTitles < 1) alertMaxTitles = Number(USER_DEFAULTS.ALERT_MAX_TITLES);

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var runSheet = ss.getSheetByName(CONFIG.SHEETS.RUN_LOG);
  var reconSheet = ss.getSheetByName(CONFIG.SHEETS.RECON);
  var sourceStatusSheet = ss.getSheetByName(CONFIG.SHEETS.SOURCE_STATUS);

  var latestRun = null;
  if (runSheet && runSheet.getLastRow() >= 2) {
    latestRun = readSheetObjects_(runSheet).slice().sort(function(a, b) {
      return String(b.started_at || '').localeCompare(String(a.started_at || ''));
    })[0] || null;
  }

  var reconRows = reconSheet ? readSheetObjects_(reconSheet) : [];
  var weeklyRows = reconRows.filter(function(r) {
    return r.record_scope === 'title' && r.record_semantics === 'title_period_gross';
  });

  var trackedTitles = unique_(weeklyRows.map(function(r) {
    return r.film_id || r.canonical_title || r.film_title_raw;
  }).filter(Boolean)).length;

  var marketsCovered = unique_(weeklyRows.map(function(r) { return r.country_code; }).filter(Boolean)).sort();

  var latestByFilmMarket = {};
  weeklyRows.forEach(function(r) {
    var filmKey = String(r.film_id || r.canonical_title || r.film_title_raw || '');
    var marketKey = String(r.country_code || '');
    if (!filmKey || !marketKey) return;
    var key = filmKey + '||' + marketKey;
    var currentSort = dashboardSortValue_(r.period_start_date, r.period_key);
    if (!latestByFilmMarket[key] || currentSort > latestByFilmMarket[key]._sort) {
      latestByFilmMarket[key] = {
        _sort: currentSort,
        title: String(r.canonical_title || r.film_title_raw || 'Untitled'),
        country_code: marketKey,
        country: String(r.country || ((CONFIG.MARKETS[marketKey] && CONFIG.MARKETS[marketKey].country) || marketKey)),
        period: dashboardPeriodLabel_(r, 0, []),
        gross_local: Number(r.period_gross_local || 0),
        gross_usd: Number(r.period_gross_usd || 0),
        currency: String(r.currency || ''),
        source_name: String(r.source_name || '')
      };
    }
  });

  var alertRows = Object.keys(latestByFilmMarket).map(function(k) { return latestByFilmMarket[k]; })
    .filter(function(r) { return Number(r.gross_usd || 0) >= threshold; })
    .sort(function(a, b) { return Number(b.gross_usd || 0) - Number(a.gross_usd || 0); })
    .slice(0, alertMaxTitles);

  if (mode === 'ALERT' && !alertRows.length && !sendIfEmpty) return false;
  if ((mode === 'DIGEST' || mode === 'BOTH') && !weeklyRows.length && !sendIfEmpty) return false;

  var statusLines = [];
  if (sourceStatusSheet && sourceStatusSheet.getLastRow() >= 2) {
    statusLines = readSheetObjects_(sourceStatusSheet).slice(-12).map(function(s) {
      return '- ' + (s.source_name || '') + ' [' + (s.country || 'All') + ']: ' + (s.status || '') + ' (' + (s.rows || 0) + ' rows)';
    });
  }

  var topMarkets = marketsCovered.map(function(code) {
    var marketRows = weeklyRows.filter(function(r) { return r.country_code === code; });
    var latestRows = {};
    marketRows.forEach(function(r) {
      var filmKey = String(r.film_id || r.canonical_title || r.film_title_raw || '');
      if (!filmKey) return;
      var currentSort = dashboardSortValue_(r.period_start_date, r.period_key);
      if (!latestRows[filmKey] || currentSort > latestRows[filmKey]._sort) {
        latestRows[filmKey] = {
          _sort: currentSort,
          title: String(r.canonical_title || r.film_title_raw || 'Untitled'),
          gross_local: Number(r.period_gross_local || 0),
          currency: String(r.currency || ''),
          period: dashboardPeriodLabel_(r, 0, [])
        };
      }
    });
    var top = Object.keys(latestRows).map(function(k) { return latestRows[k]; })
      .sort(function(a, b) { return Number(b.gross_local || 0) - Number(a.gross_local || 0); })
      .slice(0, 5);
    return { code: code, country: (CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code, top: top };
  });

  var lines = [];
  lines.push('MENA Box Office Weekly Update');
  lines.push('');
  lines.push('Mode: ' + mode);
  lines.push('Generated: ' + isoNow_());
  lines.push('Spreadsheet: ' + ss.getName());
  lines.push('');

  if (latestRun) {
    lines.push('Latest pipeline run');
    lines.push('- Run ID: ' + (latestRun.run_id || ''));
    lines.push('- Started: ' + (latestRun.started_at || ''));
    lines.push('- Completed: ' + (latestRun.completed_at || ''));
    lines.push('- Rows fetched: ' + (latestRun.rows_fetched || 0));
    lines.push('- Raw added: ' + (latestRun.raw_added || 0));
    lines.push('- Reconciled written: ' + (latestRun.reconciled_written || 0));
    lines.push('');
  }

  if (mode === 'DIGEST' || mode === 'BOTH') {
    lines.push('Coverage snapshot');
    lines.push('- Weekly title rows: ' + weeklyRows.length);
    lines.push('- Tracked titles: ' + trackedTitles);
    lines.push('- Markets covered: ' + (marketsCovered.join(', ') || 'None'));
    lines.push('');

    if (topMarkets.length) {
      lines.push('Latest market leaders');
      topMarkets.forEach(function(m) {
        lines.push(m.country + ' (' + m.code + ')');
        if (!m.top.length) {
          lines.push('  - No rows');
        } else {
          m.top.forEach(function(r, idx) {
            lines.push('  ' + (idx + 1) + '. ' + r.title + ' — ' + formatNumberForEmail_(r.gross_local) + ' ' + r.currency + ' [' + r.period + ']');
          });
        }
      });
      lines.push('');
    }
  }

  if (mode === 'ALERT' || mode === 'BOTH') {
    lines.push('Alert section');
    lines.push('- Threshold: ' + formatNumberForEmail_(threshold) + ' USD');
    if (!alertRows.length) {
      lines.push('- No titles crossed the threshold in their latest observed market period.');
    } else {
      alertRows.forEach(function(r, idx) {
        lines.push((idx + 1) + '. ' + r.title + ' — ' + formatNumberForEmail_(r.gross_usd) + ' USD (' + formatNumberForEmail_(r.gross_local) + ' ' + r.currency + ') — ' + r.country + ' [' + r.period + '] via ' + r.source_name);
      });
    }
    lines.push('');
  }

  lines.push('Recent source checks');
  if (statusLines.length) {
    statusLines.forEach(function(line) { lines.push(line); });
  } else {
    lines.push('- No recent source checks found.');
  }

  lines.push('');
  lines.push('Workbook URL:');
  lines.push(ss.getUrl());

  MailApp.sendEmail({
    to: to,
    subject: String(cfg.EMAIL_SUBJECT || USER_DEFAULTS.EMAIL_SUBJECT),
    body: lines.join('\n')
  });
  return true;
}


// ============================================================
// SECTION 17: FINAL SAFE OVERRIDES (DEDUPED MENU / CONFIG / EMAIL)
// This block is the only one that should matter for menu, config,
// weekly trigger, and email behavior.
// ============================================================

// Give the weekly pipeline a little more breathing room.
try {
  if (CONFIG && CONFIG.PIPELINE) {
    CONFIG.PIPELINE.HTTP_SLEEP_MS = 650;
  }
} catch (e) {
  Logger.log('CONFIG override warning: ' + e.message);
}

const FINAL_USER_DEFAULTS = {
  EMAIL_ENABLED: 'yes',
  EMAIL_TO: '',
  EMAIL_MODE: 'BOTH', // DIGEST | ALERT | BOTH
  EMAIL_SUBJECT: 'MENA Box Office Weekly Update',
  EMAIL_WEEKDAY: 'MONDAY',
  EMAIL_HOUR: '9',
  EMAIL_INCLUDE_PIPELINE: 'yes',
  EMAIL_SEND_IF_EMPTY: 'no',
  ALERT_THRESHOLD_USD: '500000',
  ALERT_MAX_TITLES: '12'
};

function installMenu() {
  SpreadsheetApp.getUi()
    .createMenu('🎬 MENA Intelligence')
    .addItem('Initialize Workbook', 'initializeWorkbook')
    .addSeparator()
    .addItem('Run Pipeline', 'runPipeline')
    .addItem('Run Weekly Pipeline + Email Now', 'runWeeklyPipelineEmailNow')
    .addItem('Send Weekly Email Now', 'sendWeeklyDigestNow')
    .addSeparator()
    .addItem('Search Title (Fast + Queue)', 'searchTitleFromSheet')
    .addItem('Refresh Live Lookup (Fast + Queue)', 'refreshLiveLookupFromSheet')
    .addSeparator()
    .addItem('Open Dashboard', 'openDashboard')
    .addItem('Update Dashboard', 'updateDashboardFromSheet')
    .addItem('Refresh Weekly Charts', 'refreshWeeklyChartsSheets')
    .addSeparator()
    .addItem('Process Lookup Queue', 'processLookupQueue')
    .addItem('Install Lookup Worker Trigger', 'installLookupWorkerTrigger')
    .addItem('Remove Lookup Worker Trigger', 'removeLookupWorkerTrigger')
    .addSeparator()
    .addItem('Install Weekly Pipeline + Email Trigger', 'installWeeklyPipelineEmailTrigger')
    .addItem('Remove Weekly Pipeline + Email Trigger', 'removeWeeklyPipelineEmailTrigger')
    .addToUi();
}

function onOpen() {
  installMenu();
}

function initializeWorkbook() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  ensureLookupJobsSheet_(ss);

  var lookupSheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  var dashboardSheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  var existingLookupQuery = lookupSheet ? String(lookupSheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '') : '';
  var existingDashboardQuery = dashboardSheet ? String(dashboardSheet.getRange('B2').getValue() || '') : '';
  var existingDashboardCountry = dashboardSheet ? String(dashboardSheet.getRange('B3').getValue() || 'ALL') : 'ALL';

  seedSystemConfig_(ss);
  seedTicketPrices_(ss);
  setupLookupSheet_(ss, existingLookupQuery);
  setupDashboardSheet_(ss, existingDashboardQuery || existingLookupQuery, existingDashboardCountry);
  setupVisualChartsSheet_(ss, VISUAL_SHEETS.ELCINEMA, 'elCinema Weekly Charts');
  setupVisualChartsSheet_(ss, VISUAL_SHEETS.BOM, 'Box Office Mojo Weekly Charts');
  writeLookupJobBanner_(ss, null, 'Idle');
  SpreadsheetApp.getActive().toast('Workbook initialized. Config is ready at the top of System_Config.', 'MENA Intelligence', 7);
}

function seedSystemConfig_(ss) {
  var sheet = ss.getSheetByName(CONFIG.SHEETS.CONFIG);
  if (!sheet) return;
  ensureHeader_(sheet, SCHEMAS.CONFIG);

  var existing = {};
  if (sheet.getLastRow() >= 2) {
    sheet.getRange(2, 1, sheet.getLastRow() - 1, 3).getValues().forEach(function(r) {
      var key = String(r[0] || '').trim();
      if (key) existing[key] = { value: r[1], notes: r[2] };
    });
  }

  var orderedRows = [
    ['EMAIL_ENABLED', existing.EMAIL_ENABLED ? existing.EMAIL_ENABLED.value : FINAL_USER_DEFAULTS.EMAIL_ENABLED, 'yes/no - master switch for weekly email sending'],
    ['EMAIL_TO', existing.EMAIL_TO ? existing.EMAIL_TO.value : FINAL_USER_DEFAULTS.EMAIL_TO, 'Recipient email address'],
    ['EMAIL_MODE', existing.EMAIL_MODE ? existing.EMAIL_MODE.value : FINAL_USER_DEFAULTS.EMAIL_MODE, 'DIGEST | ALERT | BOTH'],
    ['EMAIL_SUBJECT', existing.EMAIL_SUBJECT ? existing.EMAIL_SUBJECT.value : FINAL_USER_DEFAULTS.EMAIL_SUBJECT, 'Email subject line'],
    ['EMAIL_WEEKDAY', existing.EMAIL_WEEKDAY ? existing.EMAIL_WEEKDAY.value : FINAL_USER_DEFAULTS.EMAIL_WEEKDAY, 'MONDAY..SUNDAY for weekly trigger'],
    ['EMAIL_HOUR', existing.EMAIL_HOUR ? existing.EMAIL_HOUR.value : FINAL_USER_DEFAULTS.EMAIL_HOUR, 'Local script hour for weekly trigger (0-23)'],
    ['EMAIL_INCLUDE_PIPELINE', existing.EMAIL_INCLUDE_PIPELINE ? existing.EMAIL_INCLUDE_PIPELINE.value : FINAL_USER_DEFAULTS.EMAIL_INCLUDE_PIPELINE, 'yes/no - run pipeline before sending weekly email'],
    ['EMAIL_SEND_IF_EMPTY', existing.EMAIL_SEND_IF_EMPTY ? existing.EMAIL_SEND_IF_EMPTY.value : FINAL_USER_DEFAULTS.EMAIL_SEND_IF_EMPTY, 'yes/no - still send when there is no coverage / no alerts'],
    ['ALERT_THRESHOLD_USD', existing.ALERT_THRESHOLD_USD ? existing.ALERT_THRESHOLD_USD.value : FINAL_USER_DEFAULTS.ALERT_THRESHOLD_USD, 'USD threshold used for alert mode'],
    ['ALERT_MAX_TITLES', existing.ALERT_MAX_TITLES ? existing.ALERT_MAX_TITLES.value : FINAL_USER_DEFAULTS.ALERT_MAX_TITLES, 'Maximum number of alert titles listed'],
    ['version', CONFIG.VERSION, 'Workbook version'],
    ['timezone', CONFIG.TIMEZONE, 'Script timezone used by this project'],
    ['freshness_rule', 'Freshness is source-specific and not equivalent across source types', 'Informational'],
    ['ticket_estimate_rule', 'Only shown when ticket-price confidence is high and evidence is title-performance', 'Informational'],
    ['testing_tip', 'Use menu item: Run Weekly Pipeline + Email Now', 'Quick end-to-end test entry point']
  ];

  rewriteSheetObjects_(sheet, SCHEMAS.CONFIG, orderedRows.map(function(r) {
    return { key: r[0], value: r[1], notes: r[2] };
  }));
}

function normalizeWeekdayName_(value) {
  var s = String(value || 'MONDAY').trim().toUpperCase();
  var allowed = ['SUNDAY','MONDAY','TUESDAY','WEDNESDAY','THURSDAY','FRIDAY','SATURDAY'];
  return allowed.indexOf(s) >= 0 ? s : 'MONDAY';
}

function getSystemConfigMap_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(CONFIG.SHEETS.CONFIG);
  var map = {};
  if (!sheet || sheet.getLastRow() < 2) return map;
  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, 2).getValues();
  values.forEach(function(r) {
    var key = String(r[0] || '').trim();
    if (key) map[key] = r[1];
  });
  return map;
}

function getWeeklyEmailStatus_() {
  var cfg = getSystemConfigMap_();
  var enabled = String(cfg.EMAIL_ENABLED || FINAL_USER_DEFAULTS.EMAIL_ENABLED).trim().toLowerCase() === 'yes';
  var to = String(cfg.EMAIL_TO || FINAL_USER_DEFAULTS.EMAIL_TO).trim();
  var mode = String(cfg.EMAIL_MODE || FINAL_USER_DEFAULTS.EMAIL_MODE).trim().toUpperCase();
  if (['DIGEST', 'ALERT', 'BOTH'].indexOf(mode) < 0) mode = FINAL_USER_DEFAULTS.EMAIL_MODE;
  var sendIfEmpty = String(cfg.EMAIL_SEND_IF_EMPTY || FINAL_USER_DEFAULTS.EMAIL_SEND_IF_EMPTY).trim().toLowerCase() === 'yes';
  var threshold = Number(cfg.ALERT_THRESHOLD_USD || FINAL_USER_DEFAULTS.ALERT_THRESHOLD_USD);
  if (isNaN(threshold) || threshold < 0) threshold = Number(FINAL_USER_DEFAULTS.ALERT_THRESHOLD_USD);
  var alertMaxTitles = Number(cfg.ALERT_MAX_TITLES || FINAL_USER_DEFAULTS.ALERT_MAX_TITLES);
  if (isNaN(alertMaxTitles) || alertMaxTitles < 1) alertMaxTitles = Number(FINAL_USER_DEFAULTS.ALERT_MAX_TITLES);

  var status = {
    ok: false,
    enabled: enabled,
    to: to,
    mode: mode,
    sendIfEmpty: sendIfEmpty,
    threshold: threshold,
    alertMaxTitles: alertMaxTitles,
    reason: ''
  };

  if (!enabled) {
    status.reason = 'EMAIL_ENABLED is not yes';
    return status;
  }
  if (!to) {
    status.reason = 'EMAIL_TO is empty';
    return status;
  }
  status.ok = true;
  return status;
}

function installWeeklyPipelineEmailTrigger() {
  removeWeeklyPipelineEmailTrigger();
  seedSystemConfig_(SpreadsheetApp.getActiveSpreadsheet());
  var cfg = getSystemConfigMap_();
  var weekday = normalizeWeekdayName_(cfg.EMAIL_WEEKDAY || FINAL_USER_DEFAULTS.EMAIL_WEEKDAY);
  var hour = Number(cfg.EMAIL_HOUR || FINAL_USER_DEFAULTS.EMAIL_HOUR);
  if (isNaN(hour) || hour < 0 || hour > 23) hour = Number(FINAL_USER_DEFAULTS.EMAIL_HOUR);

  ScriptApp.newTrigger('weeklyPipelineAndEmail')
    .timeBased()
    .onWeekDay(ScriptApp.WeekDay[weekday])
    .atHour(hour)
    .create();

  SpreadsheetApp.getActive().toast('Weekly pipeline + email trigger installed for ' + weekday + ' at ' + hour + ':00.', 'MENA Intelligence', 7);
}

function removeWeeklyPipelineEmailTrigger() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'weeklyPipelineAndEmail') ScriptApp.deleteTrigger(t);
  });
}

function runWeeklyPipelineEmailNow() {
  var sent = weeklyPipelineAndEmail();
  SpreadsheetApp.getActive().toast(sent ? 'Weekly flow completed and email sent.' : 'Weekly flow completed but email was skipped.', 'MENA Intelligence', 7);
}

function weeklyPipelineAndEmail() {
  var cfg = getSystemConfigMap_();
  var shouldRunPipeline = String(cfg.EMAIL_INCLUDE_PIPELINE || FINAL_USER_DEFAULTS.EMAIL_INCLUDE_PIPELINE).toLowerCase() === 'yes';

  if (shouldRunPipeline) {
    try {
      runPipeline();
    } catch (e) {
      Logger.log('weeklyPipelineAndEmail runPipeline warning: ' + e.message);
    }
  }

  // IMPORTANT: do not refresh dashboard / chart sheets in the scheduled email flow.
  // The pipeline itself is already close to Apps Script's time limit.
  // Keep email delivery as the priority after fresh data is written.
  return sendWeeklyDigestEmail_();
}

function sendWeeklyDigestNow() {
  seedSystemConfig_(SpreadsheetApp.getActiveSpreadsheet());
  var status = getWeeklyEmailStatus_();
  if (!status.ok) {
    SpreadsheetApp.getActive().toast('Weekly email skipped: ' + status.reason, 'MENA Intelligence', 8);
    return false;
  }
  var sent = sendWeeklyDigestEmail_();
  SpreadsheetApp.getActive().toast(sent ? 'Weekly email sent.' : 'Weekly email skipped by content rules.', 'MENA Intelligence', 6);
  return sent;
}

function formatNumberForEmail_(value) {
  var num = Number(value || 0);
  if (!isFinite(num)) return String(value || '');
  var rounded = Math.round(num * 100) / 100;
  return rounded.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function sendWeeklyDigestEmail_() {
  var status = getWeeklyEmailStatus_();
  if (!status.ok) return false;

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var runSheet = ss.getSheetByName(CONFIG.SHEETS.RUN_LOG);
  var reconSheet = ss.getSheetByName(CONFIG.SHEETS.RECON);
  var sourceStatusSheet = ss.getSheetByName(CONFIG.SHEETS.SOURCE_STATUS);

  var latestRun = null;
  if (runSheet && runSheet.getLastRow() >= 2) {
    latestRun = readSheetObjects_(runSheet).slice().sort(function(a, b) {
      return String(b.started_at || '').localeCompare(String(a.started_at || ''));
    })[0] || null;
  }

  var reconRows = reconSheet ? readSheetObjects_(reconSheet) : [];
  var weeklyRows = reconRows.filter(function(r) {
    return r.record_scope === 'title' && r.record_semantics === 'title_period_gross';
  });

  var trackedTitles = unique_(weeklyRows.map(function(r) {
    return r.film_id || r.canonical_title || r.film_title_raw;
  }).filter(Boolean)).length;

  var marketsCovered = unique_(weeklyRows.map(function(r) { return r.country_code; }).filter(Boolean)).sort();

  var latestByFilmMarket = {};
  weeklyRows.forEach(function(r) {
    var filmKey = String(r.film_id || r.canonical_title || r.film_title_raw || '');
    var marketKey = String(r.country_code || '');
    if (!filmKey || !marketKey) return;
    var key = filmKey + '||' + marketKey;
    var currentSort = dashboardSortValue_(r.period_start_date, r.period_key);
    if (!latestByFilmMarket[key] || currentSort > latestByFilmMarket[key]._sort) {
      latestByFilmMarket[key] = {
        _sort: currentSort,
        title: String(r.canonical_title || r.film_title_raw || 'Untitled'),
        country_code: marketKey,
        country: String(r.country || ((CONFIG.MARKETS[marketKey] && CONFIG.MARKETS[marketKey].country) || marketKey)),
        period: dashboardPeriodLabel_(r, 0, []),
        gross_local: Number(r.period_gross_local || 0),
        gross_usd: Number(r.period_gross_usd || 0),
        currency: String(r.currency || ''),
        source_name: String(r.source_name || '')
      };
    }
  });

  var alertRows = Object.keys(latestByFilmMarket).map(function(k) { return latestByFilmMarket[k]; })
    .filter(function(r) { return Number(r.gross_usd || 0) >= status.threshold; })
    .sort(function(a, b) { return Number(b.gross_usd || 0) - Number(a.gross_usd || 0); })
    .slice(0, status.alertMaxTitles);

  if (status.mode === 'ALERT' && !alertRows.length && !status.sendIfEmpty) return false;
  if ((status.mode === 'DIGEST' || status.mode === 'BOTH') && !weeklyRows.length && !status.sendIfEmpty) return false;

  var statusLines = [];
  if (sourceStatusSheet && sourceStatusSheet.getLastRow() >= 2) {
    statusLines = readSheetObjects_(sourceStatusSheet).slice(-12).map(function(s) {
      return '- ' + (s.source_name || '') + ' [' + (s.country || 'All') + ']: ' + (s.status || '') + ' (' + (s.rows || 0) + ' rows)';
    });
  }

  var topMarkets = marketsCovered.map(function(code) {
    var marketRows = weeklyRows.filter(function(r) { return r.country_code === code; });
    var latestRows = {};
    marketRows.forEach(function(r) {
      var filmKey = String(r.film_id || r.canonical_title || r.film_title_raw || '');
      if (!filmKey) return;
      var currentSort = dashboardSortValue_(r.period_start_date, r.period_key);
      if (!latestRows[filmKey] || currentSort > latestRows[filmKey]._sort) {
        latestRows[filmKey] = {
          _sort: currentSort,
          title: String(r.canonical_title || r.film_title_raw || 'Untitled'),
          gross_local: Number(r.period_gross_local || 0),
          currency: String(r.currency || ''),
          period: dashboardPeriodLabel_(r, 0, [])
        };
      }
    });
    var top = Object.keys(latestRows).map(function(k) { return latestRows[k]; })
      .sort(function(a, b) { return Number(b.gross_local || 0) - Number(a.gross_local || 0); })
      .slice(0, 5);
    return { code: code, country: (CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code, top: top };
  });

  var lines = [];
  lines.push('MENA Box Office Weekly Update');
  lines.push('');
  lines.push('Mode: ' + status.mode);
  lines.push('Generated: ' + isoNow_());
  lines.push('Spreadsheet: ' + ss.getName());
  lines.push('');

  if (latestRun) {
    lines.push('Latest pipeline run');
    lines.push('- Run ID: ' + (latestRun.run_id || ''));
    lines.push('- Started: ' + (latestRun.started_at || ''));
    lines.push('- Completed: ' + (latestRun.completed_at || ''));
    lines.push('- Rows fetched: ' + (latestRun.rows_fetched || 0));
    lines.push('- Raw added: ' + (latestRun.raw_added || 0));
    lines.push('- Reconciled written: ' + (latestRun.reconciled_written || 0));
    lines.push('');
  }

  if (status.mode === 'DIGEST' || status.mode === 'BOTH') {
    lines.push('Coverage snapshot');
    lines.push('- Weekly title rows: ' + weeklyRows.length);
    lines.push('- Tracked titles: ' + trackedTitles);
    lines.push('- Markets covered: ' + (marketsCovered.join(', ') || 'None'));
    lines.push('');

    if (topMarkets.length) {
      lines.push('Latest market leaders');
      topMarkets.forEach(function(m) {
        lines.push(m.country + ' (' + m.code + ')');
        if (!m.top.length) {
          lines.push('  - No rows');
        } else {
          m.top.forEach(function(r, idx) {
            lines.push('  ' + (idx + 1) + '. ' + r.title + ' — ' + formatNumberForEmail_(r.gross_local) + ' ' + r.currency + ' [' + r.period + ']');
          });
        }
      });
      lines.push('');
    }
  }

  if (status.mode === 'ALERT' || status.mode === 'BOTH') {
    lines.push('Alert section');
    lines.push('- Threshold: ' + formatNumberForEmail_(status.threshold) + ' USD');
    if (!alertRows.length) {
      lines.push('- No titles crossed the threshold in their latest observed market period.');
    } else {
      alertRows.forEach(function(r, idx) {
        lines.push((idx + 1) + '. ' + r.title + ' — ' + formatNumberForEmail_(r.gross_usd) + ' USD (' + formatNumberForEmail_(r.gross_local) + ' ' + r.currency + ') — ' + r.country + ' [' + r.period + '] via ' + r.source_name);
      });
    }
    lines.push('');
  }

  lines.push('Recent source checks');
  if (statusLines.length) {
    statusLines.forEach(function(line) { lines.push(line); });
  } else {
    lines.push('- No recent source checks found.');
  }

  lines.push('');
  lines.push('Workbook URL:');
  lines.push(ss.getUrl());

  MailApp.sendEmail({
    to: status.to,
    subject: String(getSystemConfigMap_().EMAIL_SUBJECT || FINAL_USER_DEFAULTS.EMAIL_SUBJECT),
    body: lines.join('\n')
  });
  return true;
}


// ===== FINAL EMAIL OVERRIDES =====
function formatNumberForEmail_(value) {
  var num = Number(value || 0);
  if (!isFinite(num)) return String(value || '');
  var rounded = Math.round(num * 100) / 100;
  return rounded.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function formatCurrencyForEmail_(value, currency) {
  var amount = formatNumberForEmail_(value);
  return currency ? (amount + ' ' + currency) : amount;
}

function escapeHtmlForEmail_(str) {
  return String(str == null ? '' : str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function buildEmailMarketSnapshot_(weeklyRows, marketCode) {
  var rows = weeklyRows.filter(function(r) { return String(r.country_code || '') === String(marketCode || ''); });
  if (!rows.length) return null;

  var sortedRows = rows.slice().sort(function(a, b) {
    return dashboardSortValue_(b.period_start_date, b.period_key) - dashboardSortValue_(a.period_start_date, a.period_key);
  });

  var latestSort = dashboardSortValue_(sortedRows[0].period_start_date, sortedRows[0].period_key);
  var latestPeriodRows = sortedRows.filter(function(r) {
    return dashboardSortValue_(r.period_start_date, r.period_key) === latestSort;
  });

  var previousSort = null;
  for (var i = 0; i < sortedRows.length; i++) {
    var sv = dashboardSortValue_(sortedRows[i].period_start_date, sortedRows[i].period_key);
    if (sv < latestSort) {
      previousSort = sv;
      break;
    }
  }

  var previousPeriodRows = previousSort == null ? [] : sortedRows.filter(function(r) {
    return dashboardSortValue_(r.period_start_date, r.period_key) === previousSort;
  });

  var topLatest = latestPeriodRows.slice().sort(function(a, b) {
    return Number(b.period_gross_local || 0) - Number(a.period_gross_local || 0);
  }).slice(0, 5);

  var totalLatest = latestPeriodRows.reduce(function(sum, r) {
    return sum + Number(r.period_gross_local || 0);
  }, 0);

  var totalPrevious = previousPeriodRows.reduce(function(sum, r) {
    return sum + Number(r.period_gross_local || 0);
  }, 0);

  var deltaPct = null;
  if (totalPrevious > 0) deltaPct = ((totalLatest - totalPrevious) / totalPrevious) * 100;

  return {
    code: marketCode,
    country: (CONFIG.MARKETS[marketCode] && CONFIG.MARKETS[marketCode].country) || marketCode,
    periodLabel: dashboardPeriodLabel_(latestPeriodRows[0], 0, []),
    top: topLatest,
    totalLatest: totalLatest,
    deltaPct: deltaPct,
    currency: String((latestPeriodRows[0] && latestPeriodRows[0].currency) || '')
  };
}

function buildWeeklyDigestSubject_(baseSubject, latestRun) {
  var subject = String(baseSubject || 'MENA Box Office Weekly Update').trim();
  if (!latestRun) return subject;
  var completed = String(latestRun.completed_at || latestRun.started_at || '');
  return completed ? (subject + ' — ' + completed.slice(0, 10)) : subject;
}

function sendWeeklyDigestEmail_() {
  var status = getWeeklyEmailStatus_();
  if (!status.ok) return false;

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var runSheet = ss.getSheetByName(CONFIG.SHEETS.RUN_LOG);
  var reconSheet = ss.getSheetByName(CONFIG.SHEETS.RECON);

  var latestRun = null;
  if (runSheet && runSheet.getLastRow() >= 2) {
    latestRun = readSheetObjects_(runSheet).slice().sort(function(a, b) {
      return String(b.started_at || '').localeCompare(String(a.started_at || ''));
    })[0] || null;
  }

  var reconRows = reconSheet ? readSheetObjects_(reconSheet) : [];
  var weeklyRows = reconRows.filter(function(r) {
    return r.record_scope === 'title' && r.record_semantics === 'title_period_gross';
  });

  var trackedTitles = unique_(weeklyRows.map(function(r) {
    return r.film_id || r.canonical_title || r.film_title_raw;
  }).filter(Boolean)).length;

  var marketsCovered = unique_(weeklyRows.map(function(r) {
    return r.country_code;
  }).filter(Boolean)).sort();

  var marketSnapshots = marketsCovered.map(function(code) {
    return buildEmailMarketSnapshot_(weeklyRows, code);
  }).filter(Boolean);

  var latestByFilmMarket = {};
  weeklyRows.forEach(function(r) {
    var filmKey = String(r.film_id || r.canonical_title || r.film_title_raw || '');
    var marketKey = String(r.country_code || '');
    if (!filmKey || !marketKey) return;
    var key = filmKey + '||' + marketKey;
    var currentSort = dashboardSortValue_(r.period_start_date, r.period_key);
    if (!latestByFilmMarket[key] || currentSort > latestByFilmMarket[key]._sort) {
      latestByFilmMarket[key] = {
        _sort: currentSort,
        title: String(r.canonical_title || r.film_title_raw || 'Untitled'),
        country: String(r.country || ((CONFIG.MARKETS[marketKey] && CONFIG.MARKETS[marketKey].country) || marketKey)),
        period: dashboardPeriodLabel_(r, 0, []),
        gross_local: Number(r.period_gross_local || 0),
        gross_usd: Number(r.period_gross_usd || 0),
        currency: String(r.currency || '')
      };
    }
  });

  var alertRows = Object.keys(latestByFilmMarket).map(function(k) { return latestByFilmMarket[k]; })
    .filter(function(r) { return Number(r.gross_usd || 0) >= status.threshold; })
    .sort(function(a, b) { return Number(b.gross_usd || 0) - Number(a.gross_usd || 0); })
    .slice(0, status.alertMaxTitles);

  if (status.mode === 'ALERT' && !alertRows.length && !status.sendIfEmpty) return false;
  if ((status.mode === 'DIGEST' || status.mode === 'BOTH') && !weeklyRows.length && !status.sendIfEmpty) return false;

  var lines = [];
  lines.push('MENA Box Office Weekly Update');
  lines.push('');

  if (latestRun) {
    lines.push('Pipeline result');
    lines.push('- Completed: ' + (latestRun.completed_at || latestRun.started_at || ''));
    lines.push('- Raw rows fetched this run: ' + formatNumberForEmail_(latestRun.rows_fetched || 0));
    lines.push('- Raw rows added this run: ' + formatNumberForEmail_(latestRun.raw_added || 0));
    lines.push('- Reconciled rows written: ' + formatNumberForEmail_(latestRun.reconciled_written || 0));
    lines.push('');
  }

  if (status.mode === 'DIGEST' || status.mode === 'BOTH') {
    lines.push('Market snapshots');
    lines.push('- Markets covered: ' + (marketsCovered.join(', ') || 'None'));
    lines.push('- Tracked titles: ' + formatNumberForEmail_(trackedTitles));
    lines.push('');
    marketSnapshots.forEach(function(m) {
      lines.push(m.country + ' — ' + m.periodLabel);
      lines.push('- Total visible market gross: ' + formatCurrencyForEmail_(m.totalLatest, m.currency));
      if (m.deltaPct == null) {
        lines.push('- Versus previous tracked period: n/a');
      } else {
        var sign = m.deltaPct > 0 ? '+' : '';
        lines.push('- Versus previous tracked period: ' + sign + formatNumberForEmail_(Math.round(m.deltaPct * 10) / 10) + '%');
      }
      m.top.forEach(function(r, idx) {
        lines.push('  ' + (idx + 1) + '. ' + String(r.canonical_title || r.film_title_raw || 'Untitled') + ' — ' + formatCurrencyForEmail_(r.period_gross_local, r.currency));
      });
      lines.push('');
    });
  }

  if (status.mode === 'ALERT' || status.mode === 'BOTH') {
    lines.push('High-performing latest market periods');
    lines.push('- Threshold: ' + formatNumberForEmail_(status.threshold) + ' USD');
    if (!alertRows.length) {
      lines.push('- No titles crossed the threshold this week.');
    } else {
      alertRows.forEach(function(r, idx) {
        lines.push((idx + 1) + '. ' + r.title + ' — ' + formatNumberForEmail_(Math.round(r.gross_usd)) + ' USD (' + formatCurrencyForEmail_(r.gross_local, r.currency) + ') in ' + r.country + ' [' + r.period + ']');
      });
    }
    lines.push('');
  }

  lines.push('Open workbook');
  lines.push(ss.getUrl());

  var html = [];
  html.push('<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#222;line-height:1.45;">');
  html.push('<h2 style="margin:0 0 8px 0;">MENA Box Office Weekly Update</h2>');
  if (latestRun) {
    html.push('<p style="margin:0 0 14px 0;"><strong>Completed:</strong> ' + escapeHtmlForEmail_(latestRun.completed_at || latestRun.started_at || '') + '<br>');
    html.push('<strong>Raw rows fetched:</strong> ' + escapeHtmlForEmail_(formatNumberForEmail_(latestRun.rows_fetched || 0)) + ' &nbsp;|&nbsp; ');
    html.push('<strong>Raw rows added:</strong> ' + escapeHtmlForEmail_(formatNumberForEmail_(latestRun.raw_added || 0)) + ' &nbsp;|&nbsp; ');
    html.push('<strong>Reconciled rows written:</strong> ' + escapeHtmlForEmail_(formatNumberForEmail_(latestRun.reconciled_written || 0)) + '</p>');
  }

  if (status.mode === 'DIGEST' || status.mode === 'BOTH') {
    html.push('<h3 style="margin:16px 0 8px 0;">Market snapshots</h3>');
    html.push('<p style="margin:0 0 10px 0;">Markets covered: <strong>' + escapeHtmlForEmail_(marketsCovered.join(', ') || 'None') + '</strong> &nbsp;|&nbsp; Tracked titles: <strong>' + escapeHtmlForEmail_(formatNumberForEmail_(trackedTitles)) + '</strong></p>');
    marketSnapshots.forEach(function(m) {
      html.push('<div style="margin:0 0 16px 0;padding:10px 12px;border:1px solid #ddd;border-radius:8px;">');
      html.push('<div style="font-size:16px;font-weight:700;margin-bottom:4px;">' + escapeHtmlForEmail_(m.country) + '</div>');
      html.push('<div style="margin-bottom:8px;"><strong>Latest visible period:</strong> ' + escapeHtmlForEmail_(m.periodLabel) + '<br>');
      html.push('<strong>Total visible market gross:</strong> ' + escapeHtmlForEmail_(formatCurrencyForEmail_(m.totalLatest, m.currency)) + '<br>');
      if (m.deltaPct == null) {
        html.push('<strong>Versus previous tracked period:</strong> n/a');
      } else {
        var signHtml = m.deltaPct > 0 ? '+' : '';
        html.push('<strong>Versus previous tracked period:</strong> ' + escapeHtmlForEmail_(signHtml + formatNumberForEmail_(Math.round(m.deltaPct * 10) / 10) + '%'));
      }
      html.push('</div>');
      html.push('<table cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%;font-size:13px;">');
      html.push('<tr style="background:#f5f5f5;"><th align="left">Rank</th><th align="left">Title</th><th align="left">Period Gross</th></tr>');
      m.top.forEach(function(r, idx) {
        html.push('<tr><td>' + (idx + 1) + '</td><td>' + escapeHtmlForEmail_(String(r.canonical_title || r.film_title_raw || 'Untitled')) + '</td><td>' + escapeHtmlForEmail_(formatCurrencyForEmail_(r.period_gross_local, r.currency)) + '</td></tr>');
      });
      html.push('</table>');
      html.push('</div>');
    });
  }

  if (status.mode === 'ALERT' || status.mode === 'BOTH') {
    html.push('<h3 style="margin:16px 0 8px 0;">High-performing latest market periods</h3>');
    html.push('<p style="margin:0 0 8px 0;">Threshold: <strong>' + escapeHtmlForEmail_(formatNumberForEmail_(status.threshold)) + ' USD</strong></p>');
    if (!alertRows.length) {
      html.push('<p style="margin:0 0 10px 0;">No titles crossed the threshold this week.</p>');
    } else {
      html.push('<table cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%;font-size:13px;">');
      html.push('<tr style="background:#f5f5f5;"><th align="left">Rank</th><th align="left">Title</th><th align="left">USD</th><th align="left">Local</th><th align="left">Market</th><th align="left">Period</th></tr>');
      alertRows.forEach(function(r, idx) {
        html.push('<tr><td>' + (idx + 1) + '</td><td>' + escapeHtmlForEmail_(r.title) + '</td><td>' + escapeHtmlForEmail_(formatNumberForEmail_(Math.round(r.gross_usd))) + '</td><td>' + escapeHtmlForEmail_(formatCurrencyForEmail_(r.gross_local, r.currency)) + '</td><td>' + escapeHtmlForEmail_(r.country) + '</td><td>' + escapeHtmlForEmail_(r.period) + '</td></tr>');
      });
      html.push('</table>');
    }
  }

  html.push('<p style="margin-top:18px;"><a href="' + escapeHtmlForEmail_(ss.getUrl()) + '">Open workbook</a></p>');
  html.push('</div>');

  MailApp.sendEmail({
    to: status.to,
    subject: buildWeeklyDigestSubject_(String(getSystemConfigMap_().EMAIL_SUBJECT || FINAL_USER_DEFAULTS.EMAIL_SUBJECT), latestRun),
    body: lines.join('\n'),
    htmlBody: html.join('')
  });
  return true;
}




// ============================================================
// MASTER PRODUCT OVERRIDE — BOM WEEKEND-CENTRIC ENGINE
// This block overrides earlier functions with a cleaner final layer.
// ============================================================

const MASTER_PRODUCT = {
  VERSION: '5.1.0',
  SHEETS: {
    BOM_INDEX: 'BOM_Weekend_Index',
    BOM_DETAIL: 'BOM_Weekend_Title_Rows',
    BOM_QUEUE: 'BOM_Backfill_Status'
  },
  SCHEMAS: {
    BOM_INDEX: [
      'bom_key','market_code','market_name','year','weekend_code','weekend_num',
      'period_start_date','period_end_date','period_key','period_label',
      'top10_gross_usd','overall_gross_usd','releases','number_one_title',
      'weekend_url','detail_status','detail_last_fetched_at','detail_row_count',
      'last_seen_at','notes'
    ],
    BOM_DETAIL: [
      'bom_row_key','market_code','market_name','year','weekend_code','weekend_num',
      'period_start_date','period_end_date','period_key','period_label',
      'rank','last_week_rank','title_raw','weekend_gross_usd','pct_change_lw',
      'theaters','theater_change','avg_per_theater_usd','total_gross_usd',
      'weeks_in_release','distributor','is_new','is_estimated','weekend_url','fetched_at'
    ],
    BOM_QUEUE: ['metric','value','notes']
  },
  DEFAULTS: {
    EMAIL_ENABLED: 'yes',
    EMAIL_TO: 'replace-with-your-email@example.com',
    EMAIL_MODE: 'BOTH',
    EMAIL_SUBJECT: 'MENA Box Office Weekly Update',
    EMAIL_WEEKDAY: 'MONDAY',
    EMAIL_HOUR: '9',
    EMAIL_INCLUDE_PIPELINE: 'yes',
    EMAIL_SEND_IF_EMPTY: 'no',
    EMAIL_INCLUDE_ALERT_SECTION: 'no',
    ALERT_THRESHOLD_USD: '500000',
    ALERT_MAX_TITLES: '12',
    BOM_ENABLED: 'yes',
    BOM_MARKETS: 'AE,SA,EG,KW,BH,OM,JO,LB,QA',
    BOM_YEARS: 'CURRENT,PREVIOUS',
    BOM_MAX_DETAIL_PAGES_PER_RUN: '8',
    BOM_FORCE_RECENT_WEEKENDS: '2',
    BOM_BACKFILL_NOTES: 'Run pipeline repeatedly at first to fill weekend detail history. Scheduled weekly runs then keep the latest weekends fresh.'
  }
};

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('🎬 MENA BO Master')
    .addItem('Initialize Workbook', 'initializeWorkbook')
    .addItem('Run Pipeline', 'runPipeline')
    .addItem('Run Weekly Pipeline + Email Now', 'runWeeklyPipelineEmailNow')
    .addItem('Send Weekly Email Now', 'sendWeeklyDigestNow')
    .addSeparator()
    .addItem('Refresh Weekly Charts', 'refreshWeeklyChartsSheets')
    .addItem('Update Dashboard', 'updateDashboardFromSheet')
    .addSeparator()
    .addItem('Install Weekly Pipeline + Email Trigger', 'installWeeklyPipelineEmailTrigger')
    .addItem('Remove Weekly Pipeline + Email Trigger', 'removeWeeklyPipelineEmailTrigger')
    .addSeparator()
    .addItem('Refresh Live Lookup', 'refreshLiveLookupFromSheet')
    .addItem('Review Queue', 'openReviewQueue')
    .addItem('Test Sources', 'testSources')
    .addToUi();
}

function initializeWorkbook() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var lookupSheet = ss.getSheetByName(CONFIG.SHEETS.LOOKUP);
  var dashboardSheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  var lookupQuery = lookupSheet ? String(lookupSheet.getRange(CONFIG.LOOKUP.DEFAULT_QUERY_CELL).getValue() || '') : '';
  var dashboardQuery = dashboardSheet ? String(dashboardSheet.getRange('B2').getValue() || '') : '';
  var dashboardCountry = dashboardSheet ? String(dashboardSheet.getRange('B3').getValue() || '') : '';

  ensureAllSheets_(ss);
  seedSystemConfig_(ss);
  seedTicketPrices_(ss);
  setupLookupSheet_(ss, lookupQuery);
  setupDashboardSheet_(ss, dashboardQuery || lookupQuery, dashboardCountry);
  setupMasterBomSheets_(ss);
  SpreadsheetApp.getUi().alert('MENA BO Master initialized. Configure System_Config, then run Pipeline.');
}

function ensureAllSheets_(ss) {
  var ordered = [
    [CONFIG.SHEETS.LOOKUP, null],
    [CONFIG.SHEETS.DASHBOARD, null],
    [CONFIG.SHEETS.RAW, SCHEMAS.RAW],
    [CONFIG.SHEETS.RECON, SCHEMAS.RECON],
    [CONFIG.SHEETS.FILMS, SCHEMAS.FILMS],
    [CONFIG.SHEETS.ALIASES, SCHEMAS.ALIASES],
    [CONFIG.SHEETS.REVIEW, SCHEMAS.REVIEW],
    [CONFIG.SHEETS.RUN_LOG, SCHEMAS.RUN_LOG],
    [CONFIG.SHEETS.TICKET_PRICES, SCHEMAS.TICKET_PRICES],
    [CONFIG.SHEETS.SOURCE_STATUS, SCHEMAS.SOURCE_STATUS],
    [CONFIG.SHEETS.CONFIG, SCHEMAS.CONFIG],
    [MASTER_PRODUCT.SHEETS.BOM_INDEX, MASTER_PRODUCT.SCHEMAS.BOM_INDEX],
    [MASTER_PRODUCT.SHEETS.BOM_DETAIL, MASTER_PRODUCT.SCHEMAS.BOM_DETAIL],
    [MASTER_PRODUCT.SHEETS.BOM_QUEUE, MASTER_PRODUCT.SCHEMAS.BOM_QUEUE]
  ];

  ordered.forEach(function(item) {
    var name = item[0];
    var schema = item[1];
    var sheet = ss.getSheetByName(name);
    if (!sheet) sheet = ss.insertSheet(name);
    if (schema) ensureHeader_(sheet, schema);
  });

  setupLookupSheet_(ss);
  setupDashboardSheet_(ss);
  setupMasterBomSheets_(ss);
  try { setupVisualChartsSheet_(ss, VISUAL_SHEETS.ELCINEMA, 'elCinema Weekly Charts'); } catch (e1) {}
  try { setupVisualChartsSheet_(ss, VISUAL_SHEETS.BOM, 'BOM Weekly Charts'); } catch (e2) {}
  try { cleanupDefaultSheets_(ss); } catch (e3) {}
}

function setupMasterBomSheets_(ss) {
  var indexSheet = ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_INDEX);
  var detailSheet = ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_DETAIL);
  var statusSheet = ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_QUEUE);
  ensureHeader_(indexSheet, MASTER_PRODUCT.SCHEMAS.BOM_INDEX);
  ensureHeader_(detailSheet, MASTER_PRODUCT.SCHEMAS.BOM_DETAIL);
  ensureHeader_(statusSheet, MASTER_PRODUCT.SCHEMAS.BOM_QUEUE);

  if (!statusSheet.getRange('A2').getValue()) {
    rewriteSheetObjects_(statusSheet, MASTER_PRODUCT.SCHEMAS.BOM_QUEUE, [
      { metric: 'Purpose', value: 'Tracks BOM backfill status', notes: 'Index rows identify weekends. Detail rows store full title breakdowns.' },
      { metric: 'How it works', value: 'Pipeline first refreshes weekend index pages, then fetches only a limited number of pending weekend detail pages', notes: 'This avoids Apps Script timeouts.' },
      { metric: 'First-time loading', value: 'Run pipeline several times until pending counts fall', notes: 'The backlog clears progressively; weekly trigger then keeps recent weekends fresh.' }
    ]);
  }
}

function seedSystemConfig_(ss) {
  var sheet = ss.getSheetByName(CONFIG.SHEETS.CONFIG);
  ensureHeader_(sheet, SCHEMAS.CONFIG);

  var existing = {};
  if (sheet.getLastRow() >= 2) {
    sheet.getRange(2, 1, sheet.getLastRow() - 1, 3).getValues().forEach(function(r) {
      var key = String(r[0] || '').trim();
      if (key) existing[key] = { value: r[1], notes: r[2] };
    });
  }

  var currentYear = new Date().getFullYear();
  var orderedRows = [
    ['BOM_ENABLED', existing.BOM_ENABLED ? existing.BOM_ENABLED.value : MASTER_PRODUCT.DEFAULTS.BOM_ENABLED, 'yes/no - enables Box Office Mojo weekend-led ingestion'],
    ['BOM_MARKETS', existing.BOM_MARKETS ? existing.BOM_MARKETS.value : MASTER_PRODUCT.DEFAULTS.BOM_MARKETS, 'Comma-separated country codes for BOM weekend ingestion'],
    ['BOM_YEARS', existing.BOM_YEARS ? existing.BOM_YEARS.value : MASTER_PRODUCT.DEFAULTS.BOM_YEARS, 'Use CURRENT,PREVIOUS or explicit years like ' + currentYear + ',' + (currentYear - 1)],
    ['BOM_MAX_DETAIL_PAGES_PER_RUN', existing.BOM_MAX_DETAIL_PAGES_PER_RUN ? existing.BOM_MAX_DETAIL_PAGES_PER_RUN.value : MASTER_PRODUCT.DEFAULTS.BOM_MAX_DETAIL_PAGES_PER_RUN, 'How many weekend detail pages to ingest per run'],
    ['BOM_FORCE_RECENT_WEEKENDS', existing.BOM_FORCE_RECENT_WEEKENDS ? existing.BOM_FORCE_RECENT_WEEKENDS.value : MASTER_PRODUCT.DEFAULTS.BOM_FORCE_RECENT_WEEKENDS, 'Always refresh this many latest weekends per market-year'],
    ['BOM_BACKFILL_NOTES', existing.BOM_BACKFILL_NOTES ? existing.BOM_BACKFILL_NOTES.value : MASTER_PRODUCT.DEFAULTS.BOM_BACKFILL_NOTES, 'Informational'],
    ['EMAIL_ENABLED', existing.EMAIL_ENABLED ? existing.EMAIL_ENABLED.value : MASTER_PRODUCT.DEFAULTS.EMAIL_ENABLED, 'yes/no - master switch for weekly email sending'],
    ['EMAIL_TO', existing.EMAIL_TO ? existing.EMAIL_TO.value : MASTER_PRODUCT.DEFAULTS.EMAIL_TO, 'Recipient email address'],
    ['EMAIL_MODE', existing.EMAIL_MODE ? existing.EMAIL_MODE.value : MASTER_PRODUCT.DEFAULTS.EMAIL_MODE, 'DIGEST | ALERT | BOTH'],
    ['EMAIL_SUBJECT', existing.EMAIL_SUBJECT ? existing.EMAIL_SUBJECT.value : MASTER_PRODUCT.DEFAULTS.EMAIL_SUBJECT, 'Email subject line'],
    ['EMAIL_WEEKDAY', existing.EMAIL_WEEKDAY ? existing.EMAIL_WEEKDAY.value : MASTER_PRODUCT.DEFAULTS.EMAIL_WEEKDAY, 'MONDAY..SUNDAY for weekly trigger'],
    ['EMAIL_HOUR', existing.EMAIL_HOUR ? existing.EMAIL_HOUR.value : MASTER_PRODUCT.DEFAULTS.EMAIL_HOUR, 'Local script hour for weekly trigger (0-23)'],
    ['EMAIL_INCLUDE_PIPELINE', existing.EMAIL_INCLUDE_PIPELINE ? existing.EMAIL_INCLUDE_PIPELINE.value : MASTER_PRODUCT.DEFAULTS.EMAIL_INCLUDE_PIPELINE, 'yes/no - run pipeline before sending weekly email'],
    ['EMAIL_SEND_IF_EMPTY', existing.EMAIL_SEND_IF_EMPTY ? existing.EMAIL_SEND_IF_EMPTY.value : MASTER_PRODUCT.DEFAULTS.EMAIL_SEND_IF_EMPTY, 'yes/no - still send when there is no meaningful update'],
    ['EMAIL_INCLUDE_ALERT_SECTION', existing.EMAIL_INCLUDE_ALERT_SECTION ? existing.EMAIL_INCLUDE_ALERT_SECTION.value : MASTER_PRODUCT.DEFAULTS.EMAIL_INCLUDE_ALERT_SECTION, 'yes/no - include the threshold alert section in the weekly email'],
    ['ALERT_THRESHOLD_USD', existing.ALERT_THRESHOLD_USD ? existing.ALERT_THRESHOLD_USD.value : MASTER_PRODUCT.DEFAULTS.ALERT_THRESHOLD_USD, 'USD threshold used in alert mode'],
    ['ALERT_MAX_TITLES', existing.ALERT_MAX_TITLES ? existing.ALERT_MAX_TITLES.value : MASTER_PRODUCT.DEFAULTS.ALERT_MAX_TITLES, 'Maximum number of alert titles listed'],
    ['version', MASTER_PRODUCT.VERSION, 'Master product version'],
    ['timezone', CONFIG.TIMEZONE, 'Script timezone used by this project'],
    ['freshness_rule', 'Freshness is source-specific. BOM weekend index and weekend detail are treated as separate signals.', 'Informational'],
    ['ticket_estimate_rule', 'Only shown when ticket-price confidence is high and evidence is title-performance', 'Informational'],
    ['testing_tip', 'Use menu item: Run Weekly Pipeline + Email Now', 'Quick end-to-end test']
  ];

  rewriteSheetObjects_(sheet, SCHEMAS.CONFIG, orderedRows.map(function(r) {
    return { key: r[0], value: r[1], notes: r[2] };
  }));
}

function getSystemConfigMap_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(CONFIG.SHEETS.CONFIG);
  var map = {};
  if (!sheet || sheet.getLastRow() < 2) return map;
  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, 2).getValues();
  values.forEach(function(r) {
    var key = String(r[0] || '').trim();
    if (key) map[key] = r[1];
  });
  return map;
}

function getMasterConfigValue_(key, fallback) {
  var map = getSystemConfigMap_();
  if (map[key] !== undefined && map[key] !== '') return map[key];
  if (MASTER_PRODUCT.DEFAULTS[key] !== undefined) return MASTER_PRODUCT.DEFAULTS[key];
  return fallback !== undefined ? fallback : '';
}

function parseCsvCodes_(value) {
  return unique_(String(value || '')
    .split(',')
    .map(function(s) { return String(s || '').trim().toUpperCase(); })
    .filter(function(s) { return !!s; }));
}

function parseConfiguredYears_(value) {
  var currentYear = new Date().getFullYear();
  var out = [];
  String(value || '')
    .split(',')
    .map(function(s) { return String(s || '').trim().toUpperCase(); })
    .forEach(function(token) {
      if (!token) return;
      if (token === 'CURRENT') out.push(currentYear);
      else if (token === 'PREVIOUS') out.push(currentYear - 1);
      else {
        var y = parseInt(token, 10);
        if (!isNaN(y)) out.push(y);
      }
    });
  out = out.filter(function(y) { return y >= 1977 && y <= currentYear + 1; });
  out.sort(function(a, b) { return b - a; });
  return unique_(out);
}

function runPipeline() {
  var lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    ensureAllSheets_(ss);
    seedSystemConfig_(ss);

    var ctx = createRunContext_();
    var rawRecords = [];
    var statusRows = [];

    var bomEnabled = String(getMasterConfigValue_('BOM_ENABLED', 'yes')).toLowerCase() === 'yes';
    var bomMarkets = parseCsvCodes_(getMasterConfigValue_('BOM_MARKETS', MASTER_PRODUCT.DEFAULTS.BOM_MARKETS)).filter(function(code) {
      return !!CONFIG.MARKETS[code];
    });
    if (!bomMarkets.length) bomMarkets = ['AE'];

    var bomYears = parseConfiguredYears_(getMasterConfigValue_('BOM_YEARS', MASTER_PRODUCT.DEFAULTS.BOM_YEARS));
    if (!bomYears.length) bomYears = [new Date().getFullYear(), new Date().getFullYear() - 1];

    var maxDetailPages = parseInt(getMasterConfigValue_('BOM_MAX_DETAIL_PAGES_PER_RUN', MASTER_PRODUCT.DEFAULTS.BOM_MAX_DETAIL_PAGES_PER_RUN), 10);
    if (isNaN(maxDetailPages) || maxDetailPages < 1) maxDetailPages = 8;

    var forceRecent = parseInt(getMasterConfigValue_('BOM_FORCE_RECENT_WEEKENDS', MASTER_PRODUCT.DEFAULTS.BOM_FORCE_RECENT_WEEKENDS), 10);
    if (isNaN(forceRecent) || forceRecent < 0) forceRecent = 2;

    var elCinemaChartRows = [];
    runSource_(ctx, 'ELCINEMA_CHART', 'EG', function() {
      elCinemaChartRows = fetchElCinemaCurrentChart_();
      rawRecords.push.apply(rawRecords, elCinemaChartRows);
      statusRows.push([isoNow_(), 'ELCINEMA_CHART', 'EG', 'OK', elCinemaChartRows.length, 'Current Egypt chart fetched']);
      return elCinemaChartRows.length;
    }, statusRows);

    var workIds = unique_(elCinemaChartRows.map(function(r) { return r.work_id; }).filter(Boolean));
    workIds.slice(0, Math.max(Number(CONFIG.PIPELINE.EL_CINEMA_CHART_LIMIT || 25), 25)).forEach(function(workId) {
      runSource_(ctx, 'ELCINEMA_TITLE_BOXOFFICE', 'MULTI', function() {
        var rows = fetchElCinemaTitleBoxOffice_(workId);
        rawRecords.push.apply(rawRecords, rows);
        statusRows.push([isoNow_(), 'ELCINEMA_TITLE_BOXOFFICE', 'MULTI', 'OK', rows.length, 'work_id=' + workId]);
        return rows.length;
      }, statusRows);
    });

    if (bomEnabled) {
      var allIndexRows = loadBomIndexRows_(ss);
      var indexByKey = {};
      allIndexRows.forEach(function(r) { indexByKey[r.bom_key] = r; });

      bomMarkets.forEach(function(code) {
        bomYears.forEach(function(year) {
          runSource_(ctx, 'BOXOFFICEMOJO_WEEKEND_INDEX', code, function() {
            var indexRows = fetchBoxOfficeMojoWeekendIndex_(code, year);
            if (!indexRows.length) {
              statusRows.push([isoNow_(), 'BOXOFFICEMOJO_WEEKEND_INDEX', code, 'OK', 0, 'No weekend rows parsed for ' + year]);
              return 0;
            }

            indexRows.forEach(function(row) {
              var existing = indexByKey[row.bom_key] || {};
              var shouldRefetchRecent = false;
              if (forceRecent > 0) {
                shouldRefetchRecent = row.weekend_num && Number(row.weekend_num) > 0 && Number(row.weekend_num) >= (maxWeekendNum_(indexRows) - forceRecent + 1);
              }
              row.detail_status = resolveBomDetailStatus_(existing, row, shouldRefetchRecent);
              row.detail_last_fetched_at = existing.detail_last_fetched_at || '';
              row.detail_row_count = existing.detail_row_count || '';
              row.last_seen_at = isoNow_();
              indexByKey[row.bom_key] = row;
              rawRecords.push(buildBomIndexRawRecord_(row));
            });

            statusRows.push([isoNow_(), 'BOXOFFICEMOJO_WEEKEND_INDEX', code, 'OK', indexRows.length, 'Fetched ' + year + ' weekend index']);
            return indexRows.length;
          }, statusRows);
        });
      });

      var mergedIndexRows = Object.keys(indexByKey).map(function(k) { return indexByKey[k]; });
      mergedIndexRows.sort(compareBomIndexRowsDesc_);
      rewriteSheetObjects_(ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_INDEX), MASTER_PRODUCT.SCHEMAS.BOM_INDEX, mergedIndexRows);

      var pendingRows = selectBomPendingWeekendRows_(mergedIndexRows, maxDetailPages);
      if (pendingRows.length) {
        var detailMerged = loadBomDetailRows_(ss);
        var detailByKey = {};
        detailMerged.forEach(function(r) { detailByKey[r.bom_row_key] = r; });

        pendingRows.forEach(function(idxRow) {
          runSource_(ctx, 'BOXOFFICEMOJO_WEEKEND_DETAIL', idxRow.market_code, function() {
            var detailRows = fetchBoxOfficeMojoWeekendDetail_(idxRow.market_code, idxRow.weekend_code, idxRow.period_start_date, idxRow.period_end_date);
            detailRows.forEach(function(d) { detailByKey[d.bom_row_key] = d; rawRecords.push(buildBomDetailRawRecord_(d)); });
            markBomIndexFetched_(mergedIndexRows, idxRow.bom_key, detailRows.length);
            statusRows.push([isoNow_(), 'BOXOFFICEMOJO_WEEKEND_DETAIL', idxRow.market_code, 'OK', detailRows.length, idxRow.weekend_code]);
            return detailRows.length;
          }, statusRows);
        });

        var finalIndexRows = mergedIndexRows.slice().sort(compareBomIndexRowsDesc_);
        var finalDetailRows = Object.keys(detailByKey).map(function(k) { return detailByKey[k]; });
        finalDetailRows.sort(compareBomDetailRowsDesc_);
        rewriteSheetObjects_(ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_INDEX), MASTER_PRODUCT.SCHEMAS.BOM_INDEX, finalIndexRows);
        rewriteSheetObjects_(ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_DETAIL), MASTER_PRODUCT.SCHEMAS.BOM_DETAIL, finalDetailRows);
        updateBomBackfillStatusSheet_(ss, finalIndexRows, finalDetailRows);
      } else {
        updateBomBackfillStatusSheet_(ss, mergedIndexRows, loadBomDetailRows_(ss));
      }
    }

    ctx.rowsFetched = rawRecords.length;

    var rawWrite = appendRawEvidence_(ss, rawRecords, ctx.runId);
    ctx.rawAdded = rawWrite.added;
    ctx.rawSkipped = rawWrite.skipped;

    var rawRows = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RAW));
    var reconRows = reconcileEvidence_(ss, rawRows, ctx);
    rewriteSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RECON), SCHEMAS.RECON, reconRows);
    ctx.reconciledWritten = reconRows.length;

    appendRows_(ss.getSheetByName(CONFIG.SHEETS.SOURCE_STATUS), statusRows);
    finalizeRunLog_(ss, ctx);
    SpreadsheetApp.getActive().toast('Pipeline completed. Raw added: ' + ctx.rawAdded + ' | Reconciled: ' + ctx.reconciledWritten, 'MENA BO Master', 8);
  } finally {
    lock.releaseLock();
  }
}

function maxWeekendNum_(rows) {
  var maxVal = 0;
  (rows || []).forEach(function(r) {
    var n = parseInt(r.weekend_num || 0, 10);
    if (!isNaN(n) && n > maxVal) maxVal = n;
  });
  return maxVal;
}

function compareBomIndexRowsDesc_(a, b) {
  var as = dashboardSortValue_(a.period_start_date, a.period_key);
  var bs = dashboardSortValue_(b.period_start_date, b.period_key);
  if (bs !== as) return bs - as;
  return String(a.market_code || '').localeCompare(String(b.market_code || ''));
}

function compareBomDetailRowsDesc_(a, b) {
  var as = dashboardSortValue_(a.period_start_date, a.period_key);
  var bs = dashboardSortValue_(b.period_start_date, b.period_key);
  if (bs !== as) return bs - as;
  var ar = Number(a.rank || 0);
  var br = Number(b.rank || 0);
  return ar - br;
}

function loadBomIndexRows_(ss) {
  var sheet = ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_INDEX);
  return sheet ? readSheetObjects_(sheet) : [];
}

function loadBomDetailRows_(ss) {
  var sheet = ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_DETAIL);
  return sheet ? readSheetObjects_(sheet) : [];
}

function selectBomPendingWeekendRows_(indexRows, limit) {
  var rows = (indexRows || []).slice();
  var pending = [];
  var refresh = [];
  var staleDone = [];
  var now = new Date().getTime();
  var currentYear = new Date().getFullYear();

  rows.forEach(function(r) {
    var status = String(r.detail_status || '').toLowerCase();
    if (!status || status === 'pending' || status === 'error') {
      pending.push(r);
      return;
    }
    if (status === 'pending_refresh') {
      refresh.push(r);
      return;
    }
    if (status === 'done' && r.detail_last_fetched_at) {
      var fetchedAt = new Date(r.detail_last_fetched_at).getTime();
      if (!isNaN(fetchedAt)) {
        var age = now - fetchedAt;
        if (age > 6 * 86400000 && Number(r.year || 0) >= (currentYear - 1)) staleDone.push(r);
      }
    }
  });

  pending.sort(compareBomIndexRowsDesc_);
  refresh.sort(compareBomIndexRowsDesc_);
  staleDone.sort(compareBomIndexRowsDesc_);

  return pending.concat(refresh).concat(staleDone).slice(0, limit);
}

function resolveBomDetailStatus_(existing, row, shouldRefetchRecent) {
  var existingStatus = String((existing && existing.detail_status) || '').toLowerCase();
  var hasFetched = !!((existing && existing.detail_last_fetched_at) || '');
  var hasRows = Number((existing && existing.detail_row_count) || 0) > 0;

  if (!existing || !existing.bom_key) return 'pending';

  if (existingStatus === 'error') return 'error';
  if (!hasFetched && !hasRows) return existingStatus || 'pending';

  if (shouldRefetchRecent && existingStatus === 'done') return 'pending_refresh';

  if (existingStatus === 'pending' || existingStatus === 'pending_refresh') return existingStatus;

  return existingStatus || 'done';
}

function markBomIndexFetched_(indexRows, bomKey, rowCount) {
  (indexRows || []).forEach(function(r) {
    if (r.bom_key === bomKey) {
      r.detail_status = 'done';
      r.detail_last_fetched_at = isoNow_();
      r.detail_row_count = rowCount;
      r.last_seen_at = isoNow_();
    }
  });
}

function updateBomBackfillStatusSheet_(ss, indexRows, detailRows) {
  var byMarket = {};
  (indexRows || []).forEach(function(r) {
    var code = r.market_code || '';
    if (!byMarket[code]) byMarket[code] = { index: 0, done: 0, pending: 0 };
    byMarket[code].index++;
    if (String(r.detail_status || '').toLowerCase() === 'done') byMarket[code].done++;
    else byMarket[code].pending++;
  });

  var rows = [
    { metric: 'Last updated at', value: isoNow_(), notes: '' },
    { metric: 'Indexed weekends', value: (indexRows || []).length, notes: 'Rows in BOM_Weekend_Index' },
    { metric: 'Weekend title rows', value: (detailRows || []).length, notes: 'Rows in BOM_Weekend_Title_Rows' }
  ];

  Object.keys(byMarket).sort().forEach(function(code) {
    var m = byMarket[code];
    rows.push({
      metric: 'Market ' + code,
      value: 'indexed=' + m.index + ' | fetched=' + m.done + ' | pending=' + m.pending,
      notes: (CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code
    });
  });

  rewriteSheetObjects_(ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_QUEUE), MASTER_PRODUCT.SCHEMAS.BOM_QUEUE, rows);
}

function buildBomIndexRawRecord_(row) {
  return buildRawRecord_({
    source_name: 'BOXOFFICEMOJO_WEEKEND_INDEX',
    source_url: row.weekend_url || '',
    parser_confidence: 0.92,
    source_entity_id: row.bom_key,
    country: row.market_name || ((CONFIG.MARKETS[row.market_code] && CONFIG.MARKETS[row.market_code].country) || row.market_code),
    country_code: row.market_code || '',
    film_title_raw: '',
    release_year_hint: row.year || '',
    record_scope: 'market',
    record_granularity: 'weekend',
    record_semantics: 'market_period_gross',
    source_confidence: 0.90,
    match_confidence: '',
    evidence_type: 'market_chart_total',
    period_label_raw: row.period_label || '',
    period_start_date: row.period_start_date || '',
    period_end_date: row.period_end_date || '',
    period_key: row.period_key || '',
    rank: '',
    period_gross_local: row.overall_gross_usd || '',
    cumulative_gross_local: '',
    currency: 'USD',
    admissions_actual: '',
    work_id: '',
    distributor: '',
    notes: 'BOM market weekend total',
    raw_payload_json: JSON.stringify(row)
  });
}

function buildBomDetailRawRecord_(row) {
  return buildRawRecord_({
    source_name: 'BOXOFFICEMOJO_WEEKEND_DETAIL',
    source_url: row.weekend_url || '',
    parser_confidence: 0.92,
    source_entity_id: row.bom_row_key,
    country: row.market_name || ((CONFIG.MARKETS[row.market_code] && CONFIG.MARKETS[row.market_code].country) || row.market_code),
    country_code: row.market_code || '',
    film_title_raw: row.title_raw || '',
    release_year_hint: row.year || '',
    record_scope: 'title',
    record_granularity: 'weekend',
    record_semantics: 'title_period_gross',
    source_confidence: 0.91,
    match_confidence: '',
    evidence_type: 'title_performance',
    period_label_raw: row.period_label || '',
    period_start_date: row.period_start_date || '',
    period_end_date: row.period_end_date || '',
    period_key: row.period_key || '',
    rank: row.rank || '',
    period_gross_local: row.weekend_gross_usd || '',
    cumulative_gross_local: row.total_gross_usd || '',
    currency: 'USD',
    admissions_actual: '',
    work_id: '',
    distributor: row.distributor || '',
    notes: 'BOM weekend title breakdown',
    raw_payload_json: JSON.stringify(row)
  });
}

function extractTableRows_(html) {
  return String(html || '').match(/<tr[\s\S]*?<\/tr>/gi) || [];
}

function extractTableCells_(rowHtml) {
  return String(rowHtml || '').match(/<t[dh][^>]*>[\s\S]*?<\/t[dh]>/gi) || [];
}

function extractAnchorTexts_(html) {
  var out = [];
  String(html || '').replace(/<a[^>]*>([\s\S]*?)<\/a>/gi, function(_, inner) {
    var t = htmlToText_(inner);
    if (t) out.push(t);
    return _;
  });
  return out;
}

function extractHrefMatch_(html, regex) {
  var m = String(html || '').match(regex);
  return m ? m[1] : '';
}

function parsePctText_(text) {
  var s = normalizeWhitespace_(text);
  if (!s) return '';
  return s.replace(/\s+/g, '');
}

function parseChangeText_(text) {
  var s = normalizeWhitespace_(text);
  if (!s || s === '-') return '';
  return s.replace(/\s+/g, '');
}

function parseBomWeekendDateRangeFromPage_(html, fallbackYear) {
  var text = htmlToText_(html);
  var m = text.match(/([A-Za-z]+)\s+(\d{1,2})-(?:([A-Za-z]+)\s+)?(\d{1,2}),\s*(20\d{2})/);
  if (!m) return parseMojoWeekendLabel_('', fallbackYear);
  return parseMojoWeekendLabel_(m[1] + ' ' + m[2] + '-' + (m[3] ? m[3] + ' ' : '') + m[4], parseInt(m[5], 10));
}

function toMasterPeriodKey_(weekendCode) {
  var m = String(weekendCode || '').match(/(20\d{2})W(\d{1,2})/i);
  if (!m) return String(weekendCode || '');
  return m[1] + '-W' + pad2_(m[2]);
}

function toWeekendLabelFromCode_(weekendCode) {
  var m = String(weekendCode || '').match(/(20\d{2})W(\d{1,2})/i);
  if (!m) return String(weekendCode || '');
  return 'Weekend ' + parseInt(m[2], 10);
}

function fetchBoxOfficeMojoWeekendIndex_(countryCode, year) {
  var url = 'https://www.boxofficemojo.com/weekend/by-year/?area=' + encodeURIComponent(countryCode) + '&yr=' + encodeURIComponent(year);
  var html = fetchUrl_(url);
  var rows = extractTableRows_(html);
  var out = [];
  var seen = {};
  rows.forEach(function(rowHtml) {
    if (rowHtml.indexOf('/weekend/') < 0 || rowHtml.indexOf('area=' + countryCode) < 0) return;
    var codeMatch = rowHtml.match(/\/weekend\/(20\d{2}W\d{1,2})\/\?area=([A-Z]{2})/i);
    if (!codeMatch) return;
    var weekendCode = codeMatch[1];
    if (seen[weekendCode]) return;
    var cells = extractTableCells_(rowHtml);
    if (!cells.length) return;

    var anchors = extractAnchorTexts_(rowHtml);
    var dateLabel = htmlToText_(cells[0] || anchors[0] || '');
    var dates = parseMojoWeekendLabel_(dateLabel, year);
    var top10 = parseMoney_(htmlToText_(cells[1] || ''));
    var overall = parseMoney_(htmlToText_(cells[3] || ''));
    var releases = parseIntSafe_(htmlToText_(cells[5] || ''));
    var numberOne = '';
    if (cells.length >= 7) {
      numberOne = extractAnchorTexts_(cells[6])[0] || htmlToText_(cells[6]);
    }
    if (!numberOne && anchors.length > 1) numberOne = anchors[1];

    var weekendNum = parseInt((weekendCode.match(/W(\d{1,2})/i) || [,''])[1], 10);
    var bomKey = makeKey_([countryCode, weekendCode]);

    var derivedYear = parseInt((weekendCode.match(/(20\d{2})W/i) || [,''])[1], 10);
    out.push({
      bom_key: bomKey,
      market_code: countryCode,
      market_name: (CONFIG.MARKETS[countryCode] && CONFIG.MARKETS[countryCode].country) || countryCode,
      year: isNaN(derivedYear) ? year : derivedYear,
      weekend_code: weekendCode,
      weekend_num: isNaN(weekendNum) ? '' : weekendNum,
      period_start_date: dates ? dates.start : '',
      period_end_date: dates ? dates.end : '',
      period_key: toMasterPeriodKey_(weekendCode),
      period_label: toWeekendLabelFromCode_(weekendCode),
      top10_gross_usd: top10,
      overall_gross_usd: overall || top10,
      releases: releases,
      number_one_title: numberOne,
      weekend_url: 'https://www.boxofficemojo.com/weekend/' + weekendCode + '/?area=' + encodeURIComponent(countryCode),
      detail_status: 'pending',
      detail_last_fetched_at: '',
      detail_row_count: '',
      last_seen_at: isoNow_(),
      notes: ''
    });
    seen[weekendCode] = true;
  });

  return out;
}

function fetchBoxOfficeMojoWeekendDetail_(countryCode, weekendCode, fallbackStart, fallbackEnd) {
  var url = 'https://www.boxofficemojo.com/weekend/' + encodeURIComponent(weekendCode) + '/?area=' + encodeURIComponent(countryCode);
  var html = fetchUrl_(url);
  var rows = extractTableRows_(html);
  var pageDates = parseBomWeekendDateRangeFromPage_(html, parseInt((weekendCode.match(/(20\d{2})W/) || [,''])[1], 10));
  var startDate = pageDates && pageDates.start ? pageDates.start : (fallbackStart || '');
  var endDate = pageDates && pageDates.end ? pageDates.end : (fallbackEnd || '');
  var year = parseInt((weekendCode.match(/(20\d{2})W/) || [,''])[1], 10);
  var out = [];
  var seen = {};

  rows.forEach(function(rowHtml) {
    if (rowHtml.indexOf('/release/') < 0) return;
    var cells = extractTableCells_(rowHtml);
    if (cells.length < 9) return;

    var rank = parseIntSafe_(htmlToText_(cells[0] || ''));
    if (!rank) return;

    var title = extractAnchorTexts_(cells[2] || '')[0] || htmlToText_(cells[2] || '');
    title = normalizeWhitespace_(title).replace(/\s+†pro\.imdb\.com$/i, '').trim();
    if (!title) return;

    var lwText = normalizeWhitespace_(htmlToText_(cells[1] || ''));
    var lastWeekRank = lwText === '-' ? '' : parseIntSafe_(lwText);
    var weekendGross = parseMoney_(htmlToText_(cells[3] || ''));
    var pct = parsePctText_(htmlToText_(cells[4] || ''));
    var theaters = parseIntSafe_(htmlToText_(cells[5] || ''));
    var theaterChange = parseChangeText_(htmlToText_(cells[6] || ''));
    var avgPerTheater = parseMoney_(htmlToText_(cells[7] || ''));
    var totalGross = parseMoney_(htmlToText_(cells[8] || ''));
    var weeks = parseIntSafe_(htmlToText_(cells[9] || ''));
    var distributor = extractAnchorTexts_(cells[10] || '')[0] || htmlToText_(cells[10] || '');
    distributor = normalizeWhitespace_(distributor).replace(/\s+†pro\.imdb\.com$/i, '').trim();
    var isNew = /true/i.test(htmlToText_(cells[11] || ''));
    var isEstimated = /true/i.test(htmlToText_(cells[12] || ''));

    var rowKey = makeKey_([countryCode, weekendCode, rank, title]);
    if (seen[rowKey]) return;

    out.push({
      bom_row_key: rowKey,
      market_code: countryCode,
      market_name: (CONFIG.MARKETS[countryCode] && CONFIG.MARKETS[countryCode].country) || countryCode,
      year: isNaN(year) ? '' : year,
      weekend_code: weekendCode,
      weekend_num: parseInt((weekendCode.match(/W(\d{1,2})/i) || [,''])[1], 10),
      period_start_date: startDate,
      period_end_date: endDate,
      period_key: toMasterPeriodKey_(weekendCode),
      period_label: toWeekendLabelFromCode_(weekendCode),
      rank: rank,
      last_week_rank: lastWeekRank,
      title_raw: title,
      weekend_gross_usd: weekendGross,
      pct_change_lw: pct,
      theaters: theaters,
      theater_change: theaterChange,
      avg_per_theater_usd: avgPerTheater,
      total_gross_usd: totalGross,
      weeks_in_release: weeks,
      distributor: distributor,
      is_new: isNew ? 'yes' : 'no',
      is_estimated: isEstimated ? 'yes' : 'no',
      weekend_url: url,
      fetched_at: isoNow_()
    });
    seen[rowKey] = true;
  });

  return out;
}

function computeFreshnessStatus_(sourceName, fetchedAt) {
  var limits = {
    ELCINEMA_TITLE_BOXOFFICE: 14,
    ELCINEMA_CHART: 7,
    BOXOFFICEMOJO_WEEKEND_INDEX: 14,
    BOXOFFICEMOJO_WEEKEND_DETAIL: 14,
    BOXOFFICEMOJO_RELEASE: 14,
    BOXOFFICEMOJO_TITLE: 21,
    BOXOFFICEMOJO_INTL: 7,
    BOXOFFICEMOJO_AREA_YEAR: 30
  };
  var limit = limits[sourceName] || (CONFIG.FRESHNESS_DAYS[sourceName] || 14);
  if (!fetchedAt) return 'unknown';
  var ageDays = (new Date().getTime() - new Date(fetchedAt).getTime()) / 86400000;
  return ageDays <= limit ? 'fresh' : 'stale';
}

function getSourcePrecedencePatched_(sourceName) {
  var local = {
    ELCINEMA_TITLE_BOXOFFICE: 100,
    ELCINEMA_CHART: 90,
    BOXOFFICEMOJO_WEEKEND_DETAIL: 85,
    BOXOFFICEMOJO_WEEKEND_INDEX: 70,
    BOXOFFICEMOJO_RELEASE: 60,
    BOXOFFICEMOJO_TITLE: 55,
    BOXOFFICEMOJO_INTL: 40,
    BOXOFFICEMOJO_AREA_YEAR: 35
  };
  return local[sourceName] || CONFIG.SOURCE_PRECEDENCE[sourceName] || 0;
}

function getWeeklyEmailStatus_() {
  var cfg = getSystemConfigMap_();
  var enabled = String(cfg.EMAIL_ENABLED || MASTER_PRODUCT.DEFAULTS.EMAIL_ENABLED).trim().toLowerCase() === 'yes';
  var to = String(cfg.EMAIL_TO || MASTER_PRODUCT.DEFAULTS.EMAIL_TO).trim();
  var mode = String(cfg.EMAIL_MODE || MASTER_PRODUCT.DEFAULTS.EMAIL_MODE).trim().toUpperCase();
  if (['DIGEST', 'ALERT', 'BOTH'].indexOf(mode) < 0) mode = MASTER_PRODUCT.DEFAULTS.EMAIL_MODE;
  var sendIfEmpty = String(cfg.EMAIL_SEND_IF_EMPTY || MASTER_PRODUCT.DEFAULTS.EMAIL_SEND_IF_EMPTY).trim().toLowerCase() === 'yes';
  var threshold = Number(cfg.ALERT_THRESHOLD_USD || MASTER_PRODUCT.DEFAULTS.ALERT_THRESHOLD_USD);
  if (isNaN(threshold) || threshold < 0) threshold = Number(MASTER_PRODUCT.DEFAULTS.ALERT_THRESHOLD_USD);
  var alertMaxTitles = Number(cfg.ALERT_MAX_TITLES || MASTER_PRODUCT.DEFAULTS.ALERT_MAX_TITLES);
  if (isNaN(alertMaxTitles) || alertMaxTitles < 1) alertMaxTitles = Number(MASTER_PRODUCT.DEFAULTS.ALERT_MAX_TITLES);
  var includeAlertSection = String(cfg.EMAIL_INCLUDE_ALERT_SECTION || 'no').trim().toLowerCase() === 'yes';

  var status = { ok: false, enabled: enabled, to: to, mode: mode, sendIfEmpty: sendIfEmpty, threshold: threshold, alertMaxTitles: alertMaxTitles, includeAlertSection: includeAlertSection, reason: '' };
  if (!enabled) { status.reason = 'EMAIL_ENABLED is not yes'; return status; }
  if (!to || /replace-with-your-email/i.test(to)) { status.reason = 'EMAIL_TO is empty or still placeholder'; return status; }
  status.ok = true;
  return status;
}

function weeklyPipelineAndEmail() {
  var cfg = getSystemConfigMap_();
  var shouldRunPipeline = String(cfg.EMAIL_INCLUDE_PIPELINE || MASTER_PRODUCT.DEFAULTS.EMAIL_INCLUDE_PIPELINE).toLowerCase() === 'yes';
  if (shouldRunPipeline) {
    try { runPipeline(); } catch (e) { Logger.log('weeklyPipelineAndEmail runPipeline warning: ' + e.message); }
  }
  return sendWeeklyDigestEmail_();
}

function installWeeklyPipelineEmailTrigger() {
  removeWeeklyPipelineEmailTrigger();
  seedSystemConfig_(SpreadsheetApp.getActiveSpreadsheet());
  var cfg = getSystemConfigMap_();
  var weekday = normalizeWeekdayName_(cfg.EMAIL_WEEKDAY || MASTER_PRODUCT.DEFAULTS.EMAIL_WEEKDAY);
  var hour = Number(cfg.EMAIL_HOUR || MASTER_PRODUCT.DEFAULTS.EMAIL_HOUR);
  if (isNaN(hour) || hour < 0 || hour > 23) hour = Number(MASTER_PRODUCT.DEFAULTS.EMAIL_HOUR);

  ScriptApp.newTrigger('weeklyPipelineAndEmail').timeBased().onWeekDay(ScriptApp.WeekDay[weekday]).atHour(hour).create();
  SpreadsheetApp.getActive().toast('Weekly trigger installed for ' + weekday + ' at ' + hour + ':00.', 'MENA BO Master', 7);
}

function removeWeeklyPipelineEmailTrigger() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'weeklyPipelineAndEmail') ScriptApp.deleteTrigger(t);
  });
}

function runWeeklyPipelineEmailNow() {
  var sent = weeklyPipelineAndEmail();
  SpreadsheetApp.getActive().toast(sent ? 'Weekly flow completed and email sent.' : 'Weekly flow completed but email was skipped.', 'MENA BO Master', 7);
  return sent;
}

function sendWeeklyDigestNow() {
  seedSystemConfig_(SpreadsheetApp.getActiveSpreadsheet());
  var status = getWeeklyEmailStatus_();
  if (!status.ok) {
    SpreadsheetApp.getActive().toast('Weekly email skipped: ' + status.reason, 'MENA BO Master', 8);
    return false;
  }
  var sent = sendWeeklyDigestEmail_();
  SpreadsheetApp.getActive().toast(sent ? 'Weekly email sent.' : 'Weekly email skipped by content rules.', 'MENA BO Master', 7);
  return sent;
}

function sendWeeklyDigestEmail_() {
  var status = getWeeklyEmailStatus_();
  if (!status.ok) return false;

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var recon = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RECON) || ss.insertSheet(CONFIG.SHEETS.RECON));
  var runLog = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RUN_LOG) || ss.insertSheet(CONFIG.SHEETS.RUN_LOG));
  var latestRun = runLog.slice().sort(function(a, b) { return String(b.started_at || '').localeCompare(String(a.started_at || '')); })[0] || null;

  var marketRows = recon.filter(function(r) {
    return r.record_scope === 'market' && r.record_semantics === 'market_period_gross' && String(r.source_name || '') === 'BOXOFFICEMOJO_WEEKEND_INDEX';
  });
  var titleRows = recon.filter(function(r) {
    return r.record_scope === 'title' && r.record_semantics === 'title_period_gross';
  });

  var digest = buildEmailDigestData_(marketRows, titleRows, status.threshold, status.alertMaxTitles);
  if (!status.sendIfEmpty && !digest.hasContent) return false;

  var cfg = getSystemConfigMap_();
  var subject = String(cfg.EMAIL_SUBJECT || MASTER_PRODUCT.DEFAULTS.EMAIL_SUBJECT || 'MENA Box Office Weekly Update').trim();

  var textLines = [];
  textLines.push(subject);
  textLines.push('');
  if (latestRun) {
    textLines.push('Latest pipeline run');
    textLines.push('- Started: ' + (latestRun.started_at || ''));
    textLines.push('- Completed: ' + (latestRun.completed_at || ''));
    textLines.push('- Rows fetched: ' + formatNumberForEmail_(latestRun.rows_fetched || 0));
    textLines.push('- Raw rows added: ' + formatNumberForEmail_(latestRun.raw_added || 0));
    textLines.push('- Reconciled written: ' + formatNumberForEmail_(latestRun.reconciled_written || 0));
    textLines.push('');
  }

  digest.marketSections.forEach(function(section) {
    textLines.push(section.marketName);
    textLines.push('Latest tracked period: ' + section.periodLabel);
    textLines.push('Market total: ' + section.marketTotalLabel);
    if (section.changeLabel) textLines.push('Vs previous tracked period: ' + section.changeLabel);
    textLines.push('Top titles');
    textLines.push('Rank | Title | Gross');
    section.titles.forEach(function(t) {
      textLines.push(t.rank + ' | ' + t.title + ' | ' + t.grossLabel);
    });
    textLines.push('');
  });

  if (status.includeAlertSection && (status.mode === 'ALERT' || status.mode === 'BOTH') && digest.alerts.length) {
    textLines.push('Alert section');
    textLines.push('Threshold: ' + formatCurrencyEmail_(status.threshold, 'USD'));
    digest.alerts.forEach(function(a, idx) {
      textLines.push((idx + 1) + '. ' + a.title + ' — ' + a.grossLabel + ' — ' + a.marketName + ' [' + a.periodLabel + ']');
    });
    textLines.push('');
  }

  textLines.push('Workbook URL');
  textLines.push(ss.getUrl());

  var html = [];
  html.push('<div style="font-family:Arial,sans-serif;font-size:14px;line-height:1.5;">');
  html.push('<h2 style="margin:0 0 10px 0;">' + escapeHtmlForEmail_(subject) + '</h2>');

  if (latestRun) {
    html.push('<div style="margin-bottom:14px;padding:10px;border:1px solid #E5E7EB;border-radius:8px;">');
    html.push('<strong>Latest pipeline run</strong><br>');
    html.push('Started: ' + escapeHtmlForEmail_(String(latestRun.started_at || '')) + '<br>');
    html.push('Completed: ' + escapeHtmlForEmail_(String(latestRun.completed_at || '')) + '<br>');
    html.push('Rows fetched: ' + escapeHtmlForEmail_(formatNumberForEmail_(latestRun.rows_fetched || 0)) + ' &nbsp;|&nbsp; ');
    html.push('Raw added: ' + escapeHtmlForEmail_(formatNumberForEmail_(latestRun.raw_added || 0)) + ' &nbsp;|&nbsp; ');
    html.push('Reconciled: ' + escapeHtmlForEmail_(formatNumberForEmail_(latestRun.reconciled_written || 0)));
    html.push('</div>');
  }

  digest.marketSections.forEach(function(section) {
    html.push('<div style="margin-bottom:16px;padding:12px;border:1px solid #E5E7EB;border-radius:8px;">');
    html.push('<h3 style="margin:0 0 8px 0;">' + escapeHtmlForEmail_(section.marketName) + '</h3>');
    html.push('<div><strong>Latest tracked period:</strong> ' + escapeHtmlForEmail_(section.periodLabel) + '</div>');
    html.push('<div><strong>Market total:</strong> ' + escapeHtmlForEmail_(section.marketTotalLabel) + '</div>');
    if (section.changeLabel) html.push('<div><strong>Vs previous tracked period:</strong> ' + escapeHtmlForEmail_(section.changeLabel) + '</div>');
    html.push('<table style="margin-top:10px;border-collapse:collapse;width:100%;">');
    html.push('<tr><th style="text-align:left;border-bottom:1px solid #ddd;padding:6px;">Rank</th><th style="text-align:left;border-bottom:1px solid #ddd;padding:6px;">Title</th><th style="text-align:left;border-bottom:1px solid #ddd;padding:6px;">Gross</th></tr>');
    section.titles.forEach(function(t) {
      html.push('<tr><td style="padding:6px;border-bottom:1px solid #f0f0f0;">' + escapeHtmlForEmail_(String(t.rank)) + '</td><td style="padding:6px;border-bottom:1px solid #f0f0f0;">' + escapeHtmlForEmail_(t.title) + '</td><td style="padding:6px;border-bottom:1px solid #f0f0f0;">' + escapeHtmlForEmail_(t.grossLabel) + '</td></tr>');
    });
    html.push('</table></div>');
  });

  if (status.includeAlertSection && (status.mode === 'ALERT' || status.mode === 'BOTH') && digest.alerts.length) {
    html.push('<div style="margin-bottom:16px;padding:12px;border:1px solid #FDE68A;border-radius:8px;background:#FFFBEB;">');
    html.push('<h3 style="margin:0 0 8px 0;">Alert section</h3>');
    html.push('<div><strong>Threshold:</strong> ' + escapeHtmlForEmail_(formatCurrencyEmail_(status.threshold, 'USD')) + '</div><ol>');
    digest.alerts.forEach(function(a) {
      html.push('<li style="margin:6px 0;">' + escapeHtmlForEmail_(a.title + ' — ' + a.grossLabel + ' — ' + a.marketName + ' [' + a.periodLabel + ']') + '</li>');
    });
    html.push('</ol></div>');
  }

  html.push('<div style="margin-top:12px;"><a href="' + ss.getUrl() + '">Open workbook</a></div>');
  html.push('</div>');

  MailApp.sendEmail({
    to: status.to,
    subject: subject,
    body: textLines.join('\n'),
    htmlBody: html.join('')
  });
  return true;
}

function buildEmailDigestData_(marketRows, titleRows, thresholdUsd, alertMaxTitles) {
  var out = { marketSections: [], alerts: [], hasContent: false };
  var latestMarketByCode = {};
  var previousMarketByCode = {};
  var titleRowsByCodeAndPeriod = {};

  (titleRows || []).forEach(function(r) {
    var code = String(r.country_code || '');
    var periodKey = String(r.period_key || '');
    if (!code || !periodKey) return;
    var bucketKey = code + '||' + periodKey;
    if (!titleRowsByCodeAndPeriod[bucketKey]) titleRowsByCodeAndPeriod[bucketKey] = [];
    titleRowsByCodeAndPeriod[bucketKey].push(r);
  });

  (marketRows || []).forEach(function(r) {
    var code = String(r.country_code || '');
    if (!code) return;
    var sortVal = dashboardSortValue_(r.period_start_date, r.period_key);
    if (!latestMarketByCode[code] || sortVal > latestMarketByCode[code]._sort) {
      previousMarketByCode[code] = latestMarketByCode[code] || null;
      latestMarketByCode[code] = { row: r, _sort: sortVal };
    } else if (!previousMarketByCode[code] || sortVal > previousMarketByCode[code]._sort) {
      previousMarketByCode[code] = { row: r, _sort: sortVal };
    }
  });

  Object.keys(latestMarketByCode).sort().forEach(function(code) {
    var latest = latestMarketByCode[code].row;
    var prev = previousMarketByCode[code] ? previousMarketByCode[code].row : null;
    var bucketKey = code + '||' + String(latest.period_key || '');
    var latestTitles = (titleRowsByCodeAndPeriod[bucketKey] || []).slice().sort(function(a, b) {
      var aGross = Number(a.period_gross_local || 0);
      var bGross = Number(b.period_gross_local || 0);
      if (bGross !== aGross) return bGross - aGross;
      return String(a.canonical_title || a.film_title_raw || '').localeCompare(String(b.canonical_title || b.film_title_raw || ''));
    }).slice(0, 10).map(function(r, idx) {
      return {
        rank: idx + 1,
        title: String(r.canonical_title || r.film_title_raw || 'Untitled'),
        grossLabel: formatCurrencyEmail_(Number(r.period_gross_local || 0), String(r.currency || 'USD'))
      };
    });

    var marketTotal = Number(latest.period_gross_local || 0);
    var prevTotal = prev ? Number(prev.period_gross_local || 0) : 0;
    var hasMeaningfulContent = marketTotal > 0 || latestTitles.length > 0;
    if (!hasMeaningfulContent) return;

    var changeLabel = '';
    if (prev && prevTotal > 0) {
      var pct = ((marketTotal - prevTotal) / prevTotal) * 100;
      changeLabel = (pct >= 0 ? '+' : '') + (Math.round(pct * 10) / 10) + '%';
    }

    out.marketSections.push({
      marketCode: code,
      marketName: String(latest.country || ((CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code)),
      periodLabel: dashboardPeriodLabel_(latest, 0, []),
      marketTotalLabel: formatCurrencyEmail_(marketTotal, String(latest.currency || 'USD')),
      changeLabel: changeLabel,
      titles: latestTitles
    });
  });

  out.alerts = [];
  out.hasContent = !!out.marketSections.length;
  return out;
}

function formatCurrencyEmail_(value, currency) {
  var num = Number(value || 0);
  if (!isFinite(num)) return String(value || '');
  return formatNumberForEmail_(Math.round(num * 100) / 100) + ' ' + String(currency || '').toUpperCase();
}

function escapeHtmlForEmail_(text) {
  return String(text || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function refreshWeeklyChartsSheets_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  var recon = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RECON));

  renderWeeklyChartsSheet_(ss, VISUAL_SHEETS.ELCINEMA, {
    title: 'elCinema Weekly Charts',
    sourceFilter: function(r) {
      return String(r.source_name || '') === 'ELCINEMA_TITLE_BOXOFFICE';
    }
  }, recon);

  renderWeeklyChartsSheet_(ss, VISUAL_SHEETS.BOM, {
    title: 'BOM Weekly Charts',
    sourceFilter: function(r) {
      return String(r.source_name || '') === 'BOXOFFICEMOJO_WEEKEND_DETAIL';
    }
  }, recon);
}


// ============================================================
// MASTER PRODUCT OVERRIDE — SMART BATCH BACKFILL LAYER
// v5.2.0: separates BOM backfill from full reconciliation so manual runs do not keep
// burning time after the useful batch work is already written.
// ============================================================

const MASTER_PRODUCT_BATCH = {
  VERSION: '5.2.0'
};

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('🎬 MENA BO Master')
    .addItem('Initialize Workbook', 'initializeWorkbook')
    .addItem('Run Pipeline', 'runPipeline')
    .addItem('Run BOM Backfill Batch', 'runBomBackfillBatch')
    .addItem('Run Weekly Pipeline + Email Now', 'runWeeklyPipelineEmailNow')
    .addItem('Send Weekly Email Now', 'sendWeeklyDigestNow')
    .addSeparator()
    .addItem('Refresh Weekly Charts', 'refreshWeeklyChartsSheets')
    .addItem('Update Dashboard', 'updateDashboardFromSheet')
    .addSeparator()
    .addItem('Install Weekly Pipeline + Email Trigger', 'installWeeklyPipelineEmailTrigger')
    .addItem('Remove Weekly Pipeline + Email Trigger', 'removeWeeklyPipelineEmailTrigger')
    .addSeparator()
    .addItem('Refresh Live Lookup', 'refreshLiveLookupFromSheet')
    .addItem('Review Queue', 'openReviewQueue')
    .addItem('Test Sources', 'testSources')
    .addToUi();
}

function runPipeline() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  seedSystemConfig_(ss);

  if (shouldPreferBomBackfillBatch_(ss)) {
    return runBomBackfillBatch();
  }
  return runFullPipeline_();
}

function runBomBackfillBatch() {
  var lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    ensureAllSheets_(ss);
    seedSystemConfig_(ss);

    var ctx = createRunContext_();
    var rawRecords = [];
    var statusRows = [];
    var result = ingestBomWeekendBatch_(ss, ctx, rawRecords, statusRows, {
      maxDetailPagesOverride: getSmartBackfillPageLimit_(),
      skipRecentRefresh: false
    });

    ctx.rowsFetched = rawRecords.length;

    if (rawRecords.length) {
      var rawWrite = appendRawEvidence_(ss, rawRecords, ctx.runId);
      ctx.rawAdded = rawWrite.added;
      ctx.rawSkipped = rawWrite.skipped;
    } else {
      ctx.rawAdded = 0;
      ctx.rawSkipped = 0;
    }

    ctx.reconciledWritten = 0;
    ctx.notes = ['BOM backfill batch only; reconciliation intentionally skipped.'];
    if (statusRows.length) appendRows_(ss.getSheetByName(CONFIG.SHEETS.SOURCE_STATUS), statusRows);
    finalizeRunLog_(ss, ctx);

    var remaining = countBomBacklogRows_(result.indexRows || loadBomIndexRows_(ss));
    SpreadsheetApp.getActive().toast(
      'BOM backfill batch complete. Detail pages processed: ' + result.detailPagesProcessed +
      ' | Raw added: ' + ctx.rawAdded +
      ' | Remaining pending: ' + remaining,
      'MENA BO Master',
      8
    );
    return result;
  } finally {
    lock.releaseLock();
  }
}

function runFullPipeline_() {
  var lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    ensureAllSheets_(ss);
    seedSystemConfig_(ss);

    var ctx = createRunContext_();
    var rawRecords = [];
    var statusRows = [];

    var bomEnabled = String(getMasterConfigValue_('BOM_ENABLED', MASTER_PRODUCT.DEFAULTS.BOM_ENABLED)).toLowerCase() === 'yes';
    var bomMarkets = parseBomMarkets_();
    var bomYears = parseBomYears_();

    var forceRecent = parseInt(getMasterConfigValue_('BOM_FORCE_RECENT_WEEKENDS', MASTER_PRODUCT.DEFAULTS.BOM_FORCE_RECENT_WEEKENDS), 10);
    if (isNaN(forceRecent) || forceRecent < 0) forceRecent = 2;

    var elCinemaChartRows = [];
    runSource_(ctx, 'ELCINEMA_CHART', 'EG', function() {
      elCinemaChartRows = fetchElCinemaCurrentChart_();
      rawRecords.push.apply(rawRecords, elCinemaChartRows);
      statusRows.push([isoNow_(), 'ELCINEMA_CHART', 'EG', 'OK', elCinemaChartRows.length, 'Current Egypt chart fetched']);
      return elCinemaChartRows.length;
    }, statusRows);

    var workIds = unique_(elCinemaChartRows.map(function(r) { return r.work_id; }).filter(Boolean));
    workIds.slice(0, Math.max(Number(CONFIG.PIPELINE.EL_CINEMA_CHART_LIMIT || 25), 25)).forEach(function(workId) {
      runSource_(ctx, 'ELCINEMA_TITLE_BOXOFFICE', 'MULTI', function() {
        var rows = fetchElCinemaTitleBoxOffice_(workId);
        rawRecords.push.apply(rawRecords, rows);
        statusRows.push([isoNow_(), 'ELCINEMA_TITLE_BOXOFFICE', 'MULTI', 'OK', rows.length, 'work_id=' + workId]);
        return rows.length;
      }, statusRows);
    });

    if (bomEnabled) {
      ingestBomWeekendBatch_(ss, ctx, rawRecords, statusRows, {
        maxDetailPagesOverride: parseInt(getMasterConfigValue_('BOM_MAX_DETAIL_PAGES_PER_RUN', MASTER_PRODUCT.DEFAULTS.BOM_MAX_DETAIL_PAGES_PER_RUN), 10),
        forceRecent: forceRecent,
        skipRecentRefresh: false
      });
    }

    ctx.rowsFetched = rawRecords.length;
    var rawWrite = appendRawEvidence_(ss, rawRecords, ctx.runId);
    ctx.rawAdded = rawWrite.added;
    ctx.rawSkipped = rawWrite.skipped;

    var rawRows = readSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RAW));
    var reconRows = reconcileEvidence_(ss, rawRows, ctx);
    rewriteSheetObjects_(ss.getSheetByName(CONFIG.SHEETS.RECON), SCHEMAS.RECON, reconRows);
    ctx.reconciledWritten = reconRows.length;

    appendRows_(ss.getSheetByName(CONFIG.SHEETS.SOURCE_STATUS), statusRows);
    finalizeRunLog_(ss, ctx);
    SpreadsheetApp.getActive().toast('Full pipeline completed. Raw added: ' + ctx.rawAdded + ' | Reconciled: ' + ctx.reconciledWritten, 'MENA BO Master', 8);
    return true;
  } finally {
    lock.releaseLock();
  }
}

function ingestBomWeekendBatch_(ss, ctx, rawRecords, statusRows, options) {
  options = options || {};
  var bomEnabled = String(getMasterConfigValue_('BOM_ENABLED', MASTER_PRODUCT.DEFAULTS.BOM_ENABLED)).toLowerCase() === 'yes';
  if (!bomEnabled) {
    return { indexRows: loadBomIndexRows_(ss), detailPagesProcessed: 0, pendingRowsProcessed: [] };
  }

  var bomMarkets = parseBomMarkets_();
  var bomYears = parseBomYears_();
  var forceRecent = typeof options.forceRecent === 'number' ? options.forceRecent : parseInt(getMasterConfigValue_('BOM_FORCE_RECENT_WEEKENDS', MASTER_PRODUCT.DEFAULTS.BOM_FORCE_RECENT_WEEKENDS), 10);
  if (isNaN(forceRecent) || forceRecent < 0) forceRecent = 2;
  var maxDetailPages = typeof options.maxDetailPagesOverride === 'number' && !isNaN(options.maxDetailPagesOverride)
    ? options.maxDetailPagesOverride
    : parseInt(getMasterConfigValue_('BOM_MAX_DETAIL_PAGES_PER_RUN', MASTER_PRODUCT.DEFAULTS.BOM_MAX_DETAIL_PAGES_PER_RUN), 10);
  if (isNaN(maxDetailPages) || maxDetailPages < 1) maxDetailPages = 4;

  var allIndexRows = loadBomIndexRows_(ss);
  var indexByKey = {};
  allIndexRows.forEach(function(r) { indexByKey[r.bom_key] = r; });

  bomMarkets.forEach(function(code) {
    bomYears.forEach(function(year) {
      runSource_(ctx, 'BOXOFFICEMOJO_WEEKEND_INDEX', code, function() {
        var indexRows = fetchBoxOfficeMojoWeekendIndex_(code, year);
        if (!indexRows.length) {
          statusRows.push([isoNow_(), 'BOXOFFICEMOJO_WEEKEND_INDEX', code, 'OK', 0, 'No weekend rows parsed for ' + year]);
          return 0;
        }

        var maxWeekend = maxWeekendNum_(indexRows);
        indexRows.forEach(function(row) {
          var existing = indexByKey[row.bom_key] || {};
          var shouldRefetchRecent = false;
          if (!options.skipRecentRefresh && forceRecent > 0) {
            shouldRefetchRecent = row.weekend_num && Number(row.weekend_num) > 0 && Number(row.weekend_num) >= (maxWeekend - forceRecent + 1);
          }
          row.detail_status = resolveBomDetailStatus_(existing, row, shouldRefetchRecent);
          row.detail_last_fetched_at = existing.detail_last_fetched_at || '';
          row.detail_row_count = existing.detail_row_count || '';
          row.last_seen_at = isoNow_();
          indexByKey[row.bom_key] = row;
          rawRecords.push(buildBomIndexRawRecord_(row));
        });

        statusRows.push([isoNow_(), 'BOXOFFICEMOJO_WEEKEND_INDEX', code, 'OK', indexRows.length, 'Fetched ' + year + ' weekend index']);
        return indexRows.length;
      }, statusRows);
    });
  });

  var mergedIndexRows = Object.keys(indexByKey).map(function(k) { return indexByKey[k]; });
  mergedIndexRows.sort(compareBomIndexRowsDesc_);
  rewriteSheetObjects_(ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_INDEX), MASTER_PRODUCT.SCHEMAS.BOM_INDEX, mergedIndexRows);

  var pendingRows = selectBomPendingWeekendRows_(mergedIndexRows, maxDetailPages);
  var detailPagesProcessed = 0;

  if (pendingRows.length) {
    var detailMerged = loadBomDetailRows_(ss);
    var detailByKey = {};
    detailMerged.forEach(function(r) { detailByKey[r.bom_row_key] = r; });

    pendingRows.forEach(function(idxRow) {
      runSource_(ctx, 'BOXOFFICEMOJO_WEEKEND_DETAIL', idxRow.market_code, function() {
        var detailRows = fetchBoxOfficeMojoWeekendDetail_(idxRow.market_code, idxRow.weekend_code, idxRow.period_start_date, idxRow.period_end_date);
        detailRows.forEach(function(d) {
          detailByKey[d.bom_row_key] = d;
          rawRecords.push(buildBomDetailRawRecord_(d));
        });
        markBomIndexFetched_(mergedIndexRows, idxRow.bom_key, detailRows.length);
        statusRows.push([isoNow_(), 'BOXOFFICEMOJO_WEEKEND_DETAIL', idxRow.market_code, 'OK', detailRows.length, idxRow.weekend_code]);
        detailPagesProcessed++;
        return detailRows.length;
      }, statusRows);
    });

    var finalIndexRows = mergedIndexRows.slice().sort(compareBomIndexRowsDesc_);
    var finalDetailRows = Object.keys(detailByKey).map(function(k) { return detailByKey[k]; });
    finalDetailRows.sort(compareBomDetailRowsDesc_);
    rewriteSheetObjects_(ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_INDEX), MASTER_PRODUCT.SCHEMAS.BOM_INDEX, finalIndexRows);
    rewriteSheetObjects_(ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_DETAIL), MASTER_PRODUCT.SCHEMAS.BOM_DETAIL, finalDetailRows);
    updateBomBackfillStatusSheet_(ss, finalIndexRows, finalDetailRows);
    return { indexRows: finalIndexRows, detailRows: finalDetailRows, detailPagesProcessed: detailPagesProcessed, pendingRowsProcessed: pendingRows };
  }

  updateBomBackfillStatusSheet_(ss, mergedIndexRows, loadBomDetailRows_(ss));
  return { indexRows: mergedIndexRows, detailRows: loadBomDetailRows_(ss), detailPagesProcessed: detailPagesProcessed, pendingRowsProcessed: pendingRows };
}

function shouldPreferBomBackfillBatch_(ss) {
  var enabled = String(getMasterConfigValue_('BOM_ENABLED', MASTER_PRODUCT.DEFAULTS.BOM_ENABLED)).toLowerCase() === 'yes';
  if (!enabled) return false;
  var indexRows = loadBomIndexRows_(ss);
  if (!indexRows.length) return true;
  return countBomBacklogRows_(indexRows) > 0;
}

function countBomBacklogRows_(indexRows) {
  var count = 0;
  (indexRows || []).forEach(function(r) {
    var status = String(r.detail_status || '').toLowerCase();
    if (!status || status === 'pending' || status === 'error' || status === 'pending_refresh') count++;
  });
  return count;
}

function getSmartBackfillPageLimit_() {
  var configured = parseInt(getMasterConfigValue_('BOM_MAX_DETAIL_PAGES_PER_RUN', MASTER_PRODUCT.DEFAULTS.BOM_MAX_DETAIL_PAGES_PER_RUN), 10);
  if (isNaN(configured) || configured < 1) configured = 4;
  return Math.min(configured, 4);
}

function weeklyPipelineAndEmail() {
  var cfg = getSystemConfigMap_();
  var shouldRunPipeline = String(cfg.EMAIL_INCLUDE_PIPELINE || MASTER_PRODUCT.DEFAULTS.EMAIL_INCLUDE_PIPELINE).toLowerCase() === 'yes';
  if (shouldRunPipeline) {
    try { runFullPipeline_(); } catch (e) { Logger.log('weeklyPipelineAndEmail runFullPipeline_ warning: ' + e.message); }
  }
  return sendWeeklyDigestEmail_();
}


// ============================================================
// MASTER PRODUCT PATCH v5.2.1
// - Sync stale BOM pending_refresh statuses from detail table
// - Prefer elCinema weekly snapshots for EG and SA in email
// - Use BOM weekend snapshots for other markets in email
// - Compute alert thresholds from USD values even for non-USD local markets
// ============================================================

function syncBomIndexStatusesFromDetail_(ss, indexRows, detailRows) {
  var index = (indexRows || []).slice();
  var detail = detailRows || [];
  var counts = {};
  detail.forEach(function(r) {
    var key = String(r.market_code || '') + '|' + String(r.weekend_code || '');
    counts[key] = (counts[key] || 0) + 1;
  });

  var changed = false;
  index.forEach(function(r) {
    var key = String(r.market_code || '') + '|' + String(r.weekend_code || '');
    var count = Number(counts[key] || 0);
    var hasFetchedEvidence = !!String(r.detail_last_fetched_at || '').trim() || count > 0 || String(r.detail_status || '').toLowerCase() === 'done';
    var nextStatus = String(r.detail_status || '').toLowerCase();

    if (hasFetchedEvidence) {
      if (nextStatus !== 'done') {
        r.detail_status = 'done';
        changed = true;
      }
      if (Number(r.detail_row_count || 0) !== count) {
        r.detail_row_count = count;
        changed = true;
      }
    } else if (!nextStatus) {
      r.detail_status = 'pending';
      changed = true;
    }
  });

  if (changed && ss) {
    index.sort(compareBomIndexRowsDesc_);
    rewriteSheetObjects_(ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_INDEX), MASTER_PRODUCT.SCHEMAS.BOM_INDEX, index);
    updateBomBackfillStatusSheet_(ss, index, detail);
  }
  return index;
}

function countBomBacklogRows_(indexRows) {
  var count = 0;
  (indexRows || []).forEach(function(r) {
    var status = String(r.detail_status || '').toLowerCase();
    var hasFetchedEvidence = !!String(r.detail_last_fetched_at || '').trim() || Number(r.detail_row_count || 0) > 0 || status === 'done';
    if (!hasFetchedEvidence && (!status || status === 'pending' || status === 'error' || status === 'pending_refresh')) count++;
  });
  return count;
}

function shouldPreferBomBackfillBatch_(ss) {
  var enabled = String(getMasterConfigValue_('BOM_ENABLED', MASTER_PRODUCT.DEFAULTS.BOM_ENABLED)).toLowerCase() === 'yes';
  if (!enabled) return false;
  var indexRows = loadBomIndexRows_(ss);
  var detailRows = loadBomDetailRows_(ss);
  indexRows = syncBomIndexStatusesFromDetail_(ss, indexRows, detailRows);
  if (!indexRows.length) return true;
  return countBomBacklogRows_(indexRows) > 0;
}

function ingestBomWeekendBatch_(ss, ctx, rawRecords, statusRows, options) {
  options = options || {};
  var bomEnabled = String(getMasterConfigValue_('BOM_ENABLED', MASTER_PRODUCT.DEFAULTS.BOM_ENABLED)).toLowerCase() === 'yes';
  if (!bomEnabled) {
    return { indexRows: loadBomIndexRows_(ss), detailPagesProcessed: 0, pendingRowsProcessed: [] };
  }

  var bomMarkets = parseBomMarkets_();
  var bomYears = parseBomYears_();
  var forceRecent = typeof options.forceRecent === 'number' ? options.forceRecent : parseInt(getMasterConfigValue_('BOM_FORCE_RECENT_WEEKENDS', MASTER_PRODUCT.DEFAULTS.BOM_FORCE_RECENT_WEEKENDS), 10);
  if (isNaN(forceRecent) || forceRecent < 0) forceRecent = 2;
  var maxDetailPages = typeof options.maxDetailPagesOverride === 'number' && !isNaN(options.maxDetailPagesOverride)
    ? options.maxDetailPagesOverride
    : parseInt(getMasterConfigValue_('BOM_MAX_DETAIL_PAGES_PER_RUN', MASTER_PRODUCT.DEFAULTS.BOM_MAX_DETAIL_PAGES_PER_RUN), 10);
  if (isNaN(maxDetailPages) || maxDetailPages < 1) maxDetailPages = 4;

  var allIndexRows = syncBomIndexStatusesFromDetail_(ss, loadBomIndexRows_(ss), loadBomDetailRows_(ss));
  var indexByKey = {};
  allIndexRows.forEach(function(r) { indexByKey[r.bom_key] = r; });

  bomMarkets.forEach(function(code) {
    bomYears.forEach(function(year) {
      runSource_(ctx, 'BOXOFFICEMOJO_WEEKEND_INDEX', code, function() {
        var indexRows = fetchBoxOfficeMojoWeekendIndex_(code, year);
        if (!indexRows.length) {
          statusRows.push([isoNow_(), 'BOXOFFICEMOJO_WEEKEND_INDEX', code, 'OK', 0, 'No weekend rows parsed for ' + year]);
          return 0;
        }

        var maxWeekend = maxWeekendNum_(indexRows);
        indexRows.forEach(function(row) {
          var existing = indexByKey[row.bom_key] || {};
          var shouldRefetchRecent = false;
          if (!options.skipRecentRefresh && forceRecent > 0) {
            shouldRefetchRecent = row.weekend_num && Number(row.weekend_num) > 0 && Number(row.weekend_num) >= (maxWeekend - forceRecent + 1);
          }
          row.detail_status = resolveBomDetailStatus_(existing, row, shouldRefetchRecent);
          row.detail_last_fetched_at = existing.detail_last_fetched_at || '';
          row.detail_row_count = existing.detail_row_count || '';
          row.last_seen_at = isoNow_();
          indexByKey[row.bom_key] = row;
          rawRecords.push(buildBomIndexRawRecord_(row));
        });

        statusRows.push([isoNow_(), 'BOXOFFICEMOJO_WEEKEND_INDEX', code, 'OK', indexRows.length, 'Fetched ' + year + ' weekend index']);
        return indexRows.length;
      }, statusRows);
    });
  });

  var mergedIndexRows = Object.keys(indexByKey).map(function(k) { return indexByKey[k]; });
  mergedIndexRows = syncBomIndexStatusesFromDetail_(ss, mergedIndexRows, loadBomDetailRows_(ss));

  var pendingRows = selectBomPendingWeekendRows_(mergedIndexRows, maxDetailPages);
  var detailPagesProcessed = 0;

  if (pendingRows.length) {
    var detailMerged = loadBomDetailRows_(ss);
    var detailByKey = {};
    detailMerged.forEach(function(r) { detailByKey[r.bom_row_key] = r; });

    pendingRows.forEach(function(idxRow) {
      runSource_(ctx, 'BOXOFFICEMOJO_WEEKEND_DETAIL', idxRow.market_code, function() {
        var detailRows = fetchBoxOfficeMojoWeekendDetail_(idxRow.market_code, idxRow.weekend_code, idxRow.period_start_date, idxRow.period_end_date);
        detailRows.forEach(function(d) {
          detailByKey[d.bom_row_key] = d;
          rawRecords.push(buildBomDetailRawRecord_(d));
        });
        markBomIndexFetched_(mergedIndexRows, idxRow.bom_key, detailRows.length);
        statusRows.push([isoNow_(), 'BOXOFFICEMOJO_WEEKEND_DETAIL', idxRow.market_code, 'OK', detailRows.length, idxRow.weekend_code]);
        detailPagesProcessed++;
        return detailRows.length;
      }, statusRows);
    });

    var finalDetailRows = Object.keys(detailByKey).map(function(k) { return detailByKey[k]; });
    finalDetailRows.sort(compareBomDetailRowsDesc_);
    var finalIndexRows = syncBomIndexStatusesFromDetail_(ss, mergedIndexRows, finalDetailRows);
    finalIndexRows.sort(compareBomIndexRowsDesc_);
    rewriteSheetObjects_(ss.getSheetByName(MASTER_PRODUCT.SHEETS.BOM_DETAIL), MASTER_PRODUCT.SCHEMAS.BOM_DETAIL, finalDetailRows);
    updateBomBackfillStatusSheet_(ss, finalIndexRows, finalDetailRows);
    return { indexRows: finalIndexRows, detailRows: finalDetailRows, detailPagesProcessed: detailPagesProcessed, pendingRowsProcessed: pendingRows };
  }

  var mergedDetailRows = loadBomDetailRows_(ss);
  var syncedIndexRows = syncBomIndexStatusesFromDetail_(ss, mergedIndexRows, mergedDetailRows);
  updateBomBackfillStatusSheet_(ss, syncedIndexRows, mergedDetailRows);
  return { indexRows: syncedIndexRows, detailRows: mergedDetailRows, detailPagesProcessed: detailPagesProcessed, pendingRowsProcessed: pendingRows };
}

function formatEmailPeriodLabelPatched_(row, fallbackType) {
  var key = String((row && row.period_key) || '');
  var type = String((row && row.record_granularity) || fallbackType || '').toUpperCase();
  var m = key.match(/^(\d{4})-W(\d{1,2})$/);
  if (m) {
    return (type === 'WEEKEND' ? 'Weekend ' : 'Week ') + String(Number(m[2]));
  }
  return dashboardPeriodLabel_(row || {}, 0, []);
}

function buildElCinemaWeeklyMarketSection_(code, rows) {
  var relevant = (rows || []).filter(function(r) {
    return String(r.country_code || '') === code && String(r.source_name || '') === 'ELCINEMA_TITLE_BOXOFFICE';
  });
  if (!relevant.length) return null;

  var keys = {};
  relevant.forEach(function(r) {
    var key = String(r.period_key || '');
    var sortVal = dashboardSortValue_(r.period_start_date, r.period_key);
    if (!keys[key] || sortVal > keys[key].sortVal) keys[key] = { sortVal: sortVal };
  });
  var sortedKeys = Object.keys(keys).sort(function(a, b) { return keys[b].sortVal - keys[a].sortVal; });
  var latestKey = sortedKeys[0];
  var prevKey = sortedKeys[1] || '';

  var currentRows = relevant.filter(function(r) { return String(r.period_key || '') === latestKey; })
    .sort(function(a, b) { return Number(b.period_gross_local || 0) - Number(a.period_gross_local || 0); });
  var prevRows = prevKey ? relevant.filter(function(r) { return String(r.period_key || '') === prevKey; }) : [];

  var currentTotal = currentRows.reduce(function(sum, r) { return sum + Number(r.period_gross_local || 0); }, 0);
  var prevTotal = prevRows.reduce(function(sum, r) { return sum + Number(r.period_gross_local || 0); }, 0);
  var changeLabel = '';
  if (prevKey && prevTotal > 0) {
    var pct = ((currentTotal - prevTotal) / prevTotal) * 100;
    changeLabel = (pct >= 0 ? '+' : '') + (Math.round(pct * 10) / 10) + '%';
  }

  var first = currentRows[0] || {};
  return {
    marketCode: code,
    marketName: String(first.country || ((CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code)),
    periodLabel: formatEmailPeriodLabelPatched_(first, 'WEEK'),
    marketTotalLabel: formatCurrencyEmail_(currentTotal, String(first.currency || 'USD')),
    changeLabel: changeLabel,
    titles: currentRows.slice(0, 10).map(function(r, i) {
      return {
        rank: i + 1,
        title: String(r.canonical_title || r.film_title_raw || 'Untitled'),
        grossLabel: formatCurrencyEmail_(Number(r.period_gross_local || 0), String(r.currency || 'USD'))
      };
    })
  };
}

function buildBomWeekendMarketSection_(code, marketRows, titleRows) {
  var relevantMarkets = (marketRows || []).filter(function(r) {
    return String(r.country_code || '') === code && String(r.source_name || '') === 'BOXOFFICEMOJO_WEEKEND_INDEX';
  });
  if (!relevantMarkets.length) return null;

  relevantMarkets.sort(function(a, b) {
    return dashboardSortValue_(b.period_start_date, b.period_key) - dashboardSortValue_(a.period_start_date, a.period_key);
  });
  var latest = relevantMarkets[0];
  var prev = relevantMarkets[1] || null;
  var latestTitles = (titleRows || []).filter(function(r) {
    return String(r.country_code || '') === code &&
      String(r.source_name || '') === 'BOXOFFICEMOJO_WEEKEND_DETAIL' &&
      String(r.period_key || '') === String(latest.period_key || '');
  }).sort(function(a, b) {
    var ar = Number(a.rank || 9999), br = Number(b.rank || 9999);
    if (ar !== br) return ar - br;
    return Number(b.period_gross_local || 0) - Number(a.period_gross_local || 0);
  }).slice(0, 10);

  var marketTotal = Number(latest.period_gross_local || 0);
  var prevTotal = prev ? Number(prev.period_gross_local || 0) : 0;
  var changeLabel = '';
  if (prev && prevTotal > 0) {
    var pct = ((marketTotal - prevTotal) / prevTotal) * 100;
    changeLabel = (pct >= 0 ? '+' : '') + (Math.round(pct * 10) / 10) + '%';
  }

  return {
    marketCode: code,
    marketName: String(latest.country || ((CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code)),
    periodLabel: formatEmailPeriodLabelPatched_(latest, 'WEEKEND'),
    marketTotalLabel: formatCurrencyEmail_(marketTotal, String(latest.currency || 'USD')),
    changeLabel: changeLabel,
    titles: latestTitles.map(function(r, i) {
      return {
        rank: r.rank || (i + 1),
        title: String(r.canonical_title || r.film_title_raw || 'Untitled'),
        grossLabel: formatCurrencyEmail_(Number(r.period_gross_local || 0), String(r.currency || 'USD'))
      };
    })
  };
}

function buildEmailDigestData_(marketRows, titleRows, thresholdUsd, alertMaxTitles) {
  var out = { marketSections: [], alerts: [], hasContent: false };
  var allCodes = {};
  (marketRows || []).forEach(function(r) { allCodes[String(r.country_code || '')] = true; });
  (titleRows || []).forEach(function(r) { allCodes[String(r.country_code || '')] = true; });

  var preferredElCinemaCodes = { EG: true, SA: true };
  Object.keys(allCodes).filter(Boolean).sort().forEach(function(code) {
    var section = preferredElCinemaCodes[code]
      ? buildElCinemaWeeklyMarketSection_(code, titleRows)
      : buildBomWeekendMarketSection_(code, marketRows, titleRows);
    if (section) out.marketSections.push(section);
  });

  out.alerts = (titleRows || []).filter(function(r) {
    var code = String(r.country_code || '');
    var useRow = preferredElCinemaCodes[code]
      ? String(r.source_name || '') === 'ELCINEMA_TITLE_BOXOFFICE'
      : String(r.source_name || '') === 'BOXOFFICEMOJO_WEEKEND_DETAIL';
    if (!useRow) return false;
    var usd = Number(r.period_gross_usd || 0);
    if (!(usd > 0) && String(r.currency || '').toUpperCase() === 'USD') usd = Number(r.period_gross_local || 0);
    return usd >= Number(thresholdUsd || 0);
  }).sort(function(a, b) {
    var aSort = dashboardSortValue_(a.period_start_date, a.period_key);
    var bSort = dashboardSortValue_(b.period_start_date, b.period_key);
    if (bSort !== aSort) return bSort - aSort;
    return Number(b.period_gross_usd || 0) - Number(a.period_gross_usd || 0);
  }).slice(0, alertMaxTitles || 12).map(function(r) {
    var code = String(r.country_code || '');
    var usd = Number(r.period_gross_usd || 0);
    if (!(usd > 0) && String(r.currency || '').toUpperCase() === 'USD') usd = Number(r.period_gross_local || 0);
    var localCurrency = String(r.currency || 'USD').toUpperCase();
    var grossLabel = localCurrency === 'USD'
      ? formatCurrencyEmail_(usd, 'USD')
      : formatCurrencyEmail_(usd, 'USD') + ' (' + formatCurrencyEmail_(Number(r.period_gross_local || 0), localCurrency) + ')';
    return {
      title: String(r.canonical_title || r.film_title_raw || 'Untitled'),
      grossLabel: grossLabel,
      marketName: String(r.country || ((CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code)),
      periodLabel: formatEmailPeriodLabelPatched_(r, preferredElCinemaCodes[code] ? 'WEEK' : 'WEEKEND')
    };
  });

  out.hasContent = !!(out.marketSections.length || out.alerts.length);
  return out;
}


// ============================================================
// DASHBOARD PATCH v52 fix6
// - prevents disappearing dashboard output
// - supports BOM_Weekend_Title_Rows fallback when Reconciled_Evidence has no match
// - keeps chart helpers away from visible output
// - does not refresh weekly chart sheets during dashboard update
// ============================================================

function setupDashboardSheet_(ss, preserveQuery, preserveCountry) {
  var sheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  if (!sheet) sheet = ss.insertSheet(CONFIG.SHEETS.DASHBOARD);

  var existingQuery = preserveQuery !== undefined ? preserveQuery : String(sheet.getRange('B2').getValue() || '');
  var existingCountry = preserveCountry !== undefined ? preserveCountry : String(sheet.getRange('B3').getValue() || 'ALL');

  sheet.clear();
  try { sheet.clearCharts(); } catch (e) {}

  sheet.getRange('A1').setValue('MENA Box Office Performance Dashboard').setFontSize(15).setFontWeight('bold');
  sheet.getRange('A2').setValue('Title Query').setFontWeight('bold');
  sheet.getRange('B2').setValue(existingQuery).setBackground('#FFF9C4').setFontWeight('bold').setNote('Enter a title from Reconciled_Evidence or BOM_Weekend_Title_Rows, then run Update Dashboard.');
  sheet.getRange('A3').setValue('Country Filter').setFontWeight('bold');
  sheet.getRange('B3').setValue(existingCountry || 'ALL').setBackground('#E0F2FE').setFontWeight('bold');
  var validation = SpreadsheetApp.newDataValidation().requireValueInList(['ALL'].concat(Object.keys(CONFIG.MARKETS)), true).setAllowInvalid(false).build();
  sheet.getRange('B3').setDataValidation(validation);
  sheet.getRange('A4').setValue('Auto mode checks Reconciled_Evidence first, then falls back to BOM_Weekend_Title_Rows if needed.').setFontColor('#6B7280');
  sheet.setFrozenRows(4);
  sheet.setColumnWidths(1, 14, 150);
  sheet.setColumnWidth(2, 280);
}

function clearDashboardOutput_(sheet) {
  sheet.getRange('A6:H220').clearContent().clearFormat();
  sheet.getRange('J1:K220').clearContent().clearFormat();
  try { sheet.getRange('J:K').setFontColor('#FFFFFF'); } catch (e) {}
  var charts = sheet.getCharts();
  charts.forEach(function(chart) { sheet.removeChart(chart); });
}

function updateDashboardFromSheet() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  var sheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  var query = String(sheet.getRange('B2').getValue() || '').trim();
  var countryCode = String(sheet.getRange('B3').getValue() || 'ALL').trim().toUpperCase();
  if (!query) {
    SpreadsheetApp.getUi().alert('Enter a title in B2 on the dashboard sheet.');
    return;
  }
  renderDashboard_(query, countryCode || 'ALL');
}

function renderDashboard_(query, countryCode) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  var sheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  setupDashboardSheet_(ss, query, countryCode);
  clearDashboardOutput_(sheet);

  var chosenCountry = countryCode && countryCode !== 'ALL' ? String(countryCode).toUpperCase() : 'ALL';
  var rows = getDashboardRowsFromReconciled_(ss, query, chosenCountry);
  var sourceUsed = 'RECONCILED_EVIDENCE';

  if (!rows.length) {
    rows = getDashboardRowsFromBomDetails_(ss, query, chosenCountry);
    sourceUsed = 'BOM_WEEKEND_TITLE_ROWS';
  }

  if (!rows.length) {
    sheet.getRange('A6').setValue('No matching rows found.').setFontWeight('bold');
    sheet.getRange('A7').setValue('Tip');
    sheet.getRange('B7').setValue('Try the exact title as it appears in Reconciled_Evidence or BOM_Weekend_Title_Rows.');
    return;
  }

  var coverageCounts = {};
  rows.forEach(function(r) {
    coverageCounts[r.country_code] = (coverageCounts[r.country_code] || 0) + 1;
  });

  if (chosenCountry === 'ALL') {
    var bestCode = '';
    var bestCount = -1;
    Object.keys(coverageCounts).forEach(function(code) {
      if (coverageCounts[code] > bestCount) {
        bestCode = code;
        bestCount = coverageCounts[code];
      }
    });
    chosenCountry = bestCode || rows[0].country_code || 'ALL';
    rows = rows.filter(function(r) { return !chosenCountry || chosenCountry === 'ALL' || r.country_code === chosenCountry; });
  }

  rows.sort(function(a, b) {
    var at = dashboardSortValue_(a.period_start_date, a.period_key);
    var bt = dashboardSortValue_(b.period_start_date, b.period_key);
    if (at !== bt) return at - bt;
    return String(a.period_key || '').localeCompare(String(b.period_key || ''));
  });

  var matchedTitle = rows[0].title || query;
  var totalGross = rows.reduce(function(sum, r) { return sum + (Number(r.gross) || 0); }, 0);
  var peakRow = rows.slice().sort(function(a, b) { return Number(b.gross || 0) - Number(a.gross || 0); })[0] || null;
  var latestRow = rows.length ? rows[rows.length - 1] : null;
  var marketName = chosenCountry === 'ALL' ? 'All comparable markets' : ((CONFIG.MARKETS[chosenCountry] && CONFIG.MARKETS[chosenCountry].country) || chosenCountry);

  var summary = [
    ['Matched Title', matchedTitle, 'Source Used', sourceUsed],
    ['Country Shown', marketName, 'Records', rows.length],
    ['Total Period Gross', totalGross || '', 'Currency', rows.length ? unique_(rows.map(function(r) { return r.currency; }).filter(Boolean)).join(', ') : ''],
    ['Peak Period', peakRow ? peakRow.display_period : '', 'Peak Gross', peakRow ? peakRow.gross : ''],
    ['Latest Period', latestRow ? latestRow.display_period : '', 'Latest Gross', latestRow ? latestRow.gross : '']
  ];
  sheet.getRange(6, 1, summary.length, 4).setValues(summary);
  sheet.getRange(6, 1, 1, 4).setFontWeight('bold').setBackground('#E5E7EB');

  var noteRow = 12;
  sheet.getRange(noteRow, 1).setValue('Dashboard Notes').setFontWeight('bold').setBackground('#FDE68A');
  sheet.getRange(noteRow + 1, 1).setValue(sourceUsed === 'RECONCILED_EVIDENCE'
    ? 'Showing title-level reconciled rows. Week labels are used for elCinema weekly data and weekend labels for reconciled BOM rows.'
    : 'No reconciled title rows matched, so the dashboard is using raw Box Office Mojo weekend detail rows directly.');

  var marketSummaryRow = noteRow + 3;
  sheet.getRange(marketSummaryRow, 1).setValue('Available Market Coverage').setFontWeight('bold').setBackground('#DBEAFE');
  var marketHeader = [['Country', 'Code', 'Records', 'First Period', 'Last Period']];
  sheet.getRange(marketSummaryRow + 1, 1, 1, marketHeader[0].length).setValues(marketHeader).setFontWeight('bold');

  var allRowsForCoverage = sourceUsed === 'RECONCILED_EVIDENCE' ? getDashboardRowsFromReconciled_(ss, query, 'ALL') : getDashboardRowsFromBomDetails_(ss, query, 'ALL');
  var byMarket = {};
  allRowsForCoverage.forEach(function(r) {
    if (!byMarket[r.country_code]) byMarket[r.country_code] = [];
    byMarket[r.country_code].push(r);
  });

  var marketTable = Object.keys(byMarket).sort().map(function(code) {
    var subset = byMarket[code].slice().sort(function(a, b) {
      return dashboardSortValue_(a.period_start_date, a.period_key) - dashboardSortValue_(b.period_start_date, b.period_key);
    });
    return [
      (CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || subset[0].country || code,
      code,
      subset.length,
      subset.length ? subset[0].display_period : '',
      subset.length ? subset[subset.length - 1].display_period : ''
    ];
  });
  if (marketTable.length) {
    sheet.getRange(marketSummaryRow + 2, 1, marketTable.length, marketHeader[0].length).setValues(marketTable);
  } else {
    sheet.getRange(marketSummaryRow + 2, 1).setValue('No market coverage found.');
  }

  var weeklyRow = marketSummaryRow + 4 + Math.max(marketTable.length, 1);
  sheet.getRange(weeklyRow, 1).setValue('Performance Table').setFontWeight('bold').setBackground('#D1FAE5');
  var weeklyHeader = [['Period', 'Start Date', 'End Date', 'Country', 'Gross', 'Currency', 'Source']];
  sheet.getRange(weeklyRow + 1, 1, 1, weeklyHeader[0].length).setValues(weeklyHeader).setFontWeight('bold');
  var weeklyValues = rows.map(function(r) {
    return [r.display_period, r.period_start_date, r.period_end_date, r.country, r.gross, r.currency, r.source_name];
  });
  sheet.getRange(weeklyRow + 2, 1, weeklyValues.length, weeklyHeader[0].length).setValues(weeklyValues);

  var helperHeader = [['Period', 'Gross']];
  var helperValues = rows.map(function(r) { return [r.display_period, Number(r.gross) || 0]; });
  sheet.getRange(1, 10, 1, 2).setValues(helperHeader);
  if (helperValues.length) {
    sheet.getRange(2, 10, helperValues.length, 2).setValues(helperValues);
  }

  var chart = sheet.newChart()
    .asLineChart()
    .addRange(sheet.getRange(1, 10, helperValues.length + 1, 2))
    .setPosition(6, 6, 0, 0)
    .setOption('title', 'Performance Trend')
    .setOption('legend', { position: 'none' })
    .setOption('hAxis', { title: 'Period' })
    .setOption('vAxis', { title: 'Gross (' + (rows[0].currency || 'Local') + ')' })
    .build();
  sheet.insertChart(chart);

  try { sheet.hideColumns(10, 2); } catch (e) {}

  if (weeklyValues.length) {
    sheet.getRange(weeklyRow + 2, 2, weeklyValues.length, 2).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(weeklyRow + 2, 5, weeklyValues.length, 1).setNumberFormat('#,##0.00');
  }
  autoResize_(sheet, 12);
}

function getDashboardRowsFromReconciled_(ss, query, countryCode) {
  var sheet = ss.getSheetByName(CONFIG.SHEETS.RECON);
  if (!sheet) return [];
  var rows = readSheetObjects_(sheet);
  var normQueryEn = normalizeDashboardTitle_(query);
  var normQueryAr = normalizeArabicTitle_(query);
  var out = [];

  rows.forEach(function(r) {
    if (!(r.record_scope === 'title' && r.record_semantics === 'title_period_gross')) return;
    if (countryCode && countryCode !== 'ALL' && String(r.country_code || '').toUpperCase() !== countryCode) return;

    var title = String(r.canonical_title || r.film_title_raw || '').trim();
    var titleAr = String(r.canonical_title_ar || r.film_title_ar_raw || '').trim();
    var normTitle = normalizeDashboardTitle_(title);
    var normTitleAr = normalizeArabicTitle_(titleAr);
    var matched = false;

    if (normQueryEn && normTitle) {
      matched = normTitle === normQueryEn || normTitle.indexOf(normQueryEn) >= 0 || normQueryEn.indexOf(normTitle) >= 0 || titleSimilarity_(normQueryEn, normTitle) >= 0.82;
    }
    if (!matched && normQueryAr && normTitleAr) {
      matched = normTitleAr === normQueryAr || normTitleAr.indexOf(normQueryAr) >= 0 || normQueryAr.indexOf(normTitleAr) >= 0 || titleSimilarity_(normQueryAr, normTitleAr) >= 0.85;
    }
    if (!matched) return;

    out.push({
      title: title || titleAr || query,
      country: r.country || '',
      country_code: String(r.country_code || '').toUpperCase(),
      period_start_date: normalizeDashboardDateValue_(r.period_start_date),
      period_end_date: normalizeDashboardDateValue_(r.period_end_date),
      period_key: String(r.period_key || r.period_label_raw || ''),
      display_period: dashboardPeriodLabel_(r, 0, []),
      gross: Number(r.period_gross_local || 0),
      currency: r.currency || '',
      source_name: r.source_name || 'RECONCILED_EVIDENCE'
    });
  });

  return dedupeDashboardRows_(out);
}

function getDashboardRowsFromBomDetails_(ss, query, countryCode) {
  var sheet = ss.getSheetByName('BOM_Weekend_Title_Rows');
  if (!sheet) return [];
  var rows = readSheetObjects_(sheet);
  var normQuery = normalizeDashboardTitle_(query);
  var out = [];

  rows.forEach(function(r) {
    var code = String(r.market_code || r.country_code || '').toUpperCase();
    if (countryCode && countryCode !== 'ALL' && code !== countryCode) return;

    var title = String(r.title_raw || r.title || '').trim();
    var normTitle = normalizeDashboardTitle_(title);
    if (!(normTitle === normQuery || normTitle.indexOf(normQuery) >= 0 || normQuery.indexOf(normTitle) >= 0 || titleSimilarity_(normQuery, normTitle) >= 0.82)) return;

    out.push({
      title: title || query,
      country: r.market_name || ((CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code),
      country_code: code,
      period_start_date: normalizeDashboardDateValue_(r.period_start_date),
      period_end_date: normalizeDashboardDateValue_(r.period_end_date),
      period_key: String(r.period_key || r.weekend_code || ''),
      display_period: formatDashboardPeriodLabelFromKey_(r.period_key || r.weekend_code || ''),
      gross: Number(r.weekend_gross_usd || r.gross_usd || 0),
      currency: 'USD',
      source_name: 'BOXOFFICEMOJO_WEEKEND_DETAIL'
    });
  });

  return dedupeDashboardRows_(out);
}

function dedupeDashboardRows_(rows) {
  var seen = {};
  var out = [];
  rows.forEach(function(r) {
    var key = [r.country_code, r.period_key, r.title, r.gross].join('|');
    if (seen[key]) return;
    seen[key] = true;
    out.push(r);
  });
  return out;
}

function normalizeDashboardTitle_(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function formatDashboardPeriodLabelFromKey_(key) {
  var k = String(key || '').trim();
  var week = k.match(/^(\d{4})-W(\d{1,2})$/i);
  if (week) return 'Week ' + parseInt(week[2], 10);
  var weekend = k.match(/^(\d{4})W(\d{1,2})$/i);
  if (weekend) return 'Weekend ' + parseInt(weekend[2], 10);
  return k;
}

// ============================================================
// DASHBOARD PATCH v52 fix7 PRO
// - prefers richer market-level source (BOM vs RECON) instead of blindly preferring RECON
// - Available Market Coverage combines both sources
// - production-style controls: title dropdown, country dropdown, source mode dropdown
// - auto-refreshes on edit of dashboard controls
// ============================================================

function onEdit(e) {
  try {
    if (!e || !e.range || !e.source) return;
    var sheet = e.range.getSheet();
    if (!sheet || sheet.getName() !== CONFIG.SHEETS.DASHBOARD) return;
    var a1 = e.range.getA1Notation();
    if (['B2','B3','B4'].indexOf(a1) >= 0) {
      updateDashboardFromSheet();
    }
  } catch (err) {
    try { Logger.log('onEdit dashboard refresh failed: ' + err); } catch (e2) {}
  }
}

function setupDashboardSheet_(ss, preserveTitle, preserveCountry, preserveMode) {
  var sheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  if (!sheet) sheet = ss.insertSheet(CONFIG.SHEETS.DASHBOARD);
  var titleVal = preserveTitle !== undefined ? preserveTitle : String(sheet.getRange('B2').getValue() || '');
  var countryVal = preserveCountry !== undefined ? preserveCountry : String(sheet.getRange('B3').getValue() || 'ALL');
  var modeVal = preserveMode !== undefined ? preserveMode : String(sheet.getRange('B4').getValue() || 'AUTO');

  sheet.clear();
  try { sheet.clearCharts(); } catch (e) {}

  sheet.getRange('A1').setValue('MENA Box Office Performance Dashboard').setFontSize(15).setFontWeight('bold');
  sheet.getRange('A2').setValue('Title').setFontWeight('bold');
  sheet.getRange('A3').setValue('Country').setFontWeight('bold');
  sheet.getRange('A4').setValue('Source Mode').setFontWeight('bold');

  buildDashboardTitleCatalog_(ss);
  var listSheet = ss.getSheetByName('_Dashboard_Lists');
  var lastListRow = Math.max(listSheet.getLastRow(), 2);
  var titleValidation = SpreadsheetApp.newDataValidation()
    .requireValueInRange(listSheet.getRange(2, 1, Math.max(lastListRow - 1, 1), 1), true)
    .setAllowInvalid(true)
    .build();
  sheet.getRange('B2').setValue(titleVal).setBackground('#FFF9C4').setFontWeight('bold').setDataValidation(titleValidation).setNote('Pick a title from the dropdown or type a close match. Dashboard auto-refreshes when you change Title, Country, or Source Mode.');

  var countries = ['ALL'].concat(Object.keys(CONFIG.MARKETS));
  var countryValidation = SpreadsheetApp.newDataValidation().requireValueInList(countries, true).setAllowInvalid(false).build();
  sheet.getRange('B3').setValue(countryVal || 'ALL').setBackground('#E0F2FE').setFontWeight('bold').setDataValidation(countryValidation);

  var modeValidation = SpreadsheetApp.newDataValidation().requireValueInList(['AUTO','RECON','BOM'], true).setAllowInvalid(false).build();
  sheet.getRange('B4').setValue(modeVal || 'AUTO').setBackground('#EDE9FE').setFontWeight('bold').setDataValidation(modeValidation);

  sheet.getRange('A5').setValue('AUTO picks the strongest source per market. RECON forces Reconciled_Evidence only. BOM forces BOM_Weekend_Title_Rows only.').setFontColor('#6B7280');
  sheet.setFrozenRows(5);
  sheet.setColumnWidths(1, 14, 150);
  sheet.setColumnWidth(2, 320);
  try { sheet.hideColumns(10, 3); } catch (e) {}
}

function buildDashboardTitleCatalog_(ss) {
  var sheet = ss.getSheetByName('_Dashboard_Lists');
  if (!sheet) sheet = ss.insertSheet('_Dashboard_Lists');
  sheet.clear();
  var titles = {};

  var recon = ss.getSheetByName(CONFIG.SHEETS.RECON);
  if (recon && recon.getLastRow() > 1) {
    var rrows = readSheetObjects_(recon);
    rrows.forEach(function(r) {
      if (!(r.record_scope === 'title' && r.record_semantics === 'title_period_gross')) return;
      var t = String(r.canonical_title || r.film_title_raw || '').trim();
      if (t) titles[t] = true;
    });
  }

  var bom = ss.getSheetByName('BOM_Weekend_Title_Rows');
  if (bom && bom.getLastRow() > 1) {
    var brows = readSheetObjects_(bom);
    brows.forEach(function(r) {
      var t = String(r.title_raw || r.title || '').trim();
      if (t) titles[t] = true;
    });
  }

  var list = Object.keys(titles).sort(function(a,b){ return a.localeCompare(b); });
  if (!list.length) list = [''];
  sheet.getRange(1,1).setValue('title');
  sheet.getRange(2,1,list.length,1).setValues(list.map(function(t){ return [t]; }));
  try { sheet.hideSheet(); } catch (e) {}
}

function clearDashboardOutput_(sheet) {
  sheet.getRange('A7:H260').clearContent().clearFormat();
  sheet.getRange('J1:L260').clearContent().clearFormat();
  var charts = sheet.getCharts();
  charts.forEach(function(chart) { sheet.removeChart(chart); });
}

function updateDashboardFromSheet() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  var sheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  var title = String(sheet.getRange('B2').getValue() || '').trim();
  var country = String(sheet.getRange('B3').getValue() || 'ALL').trim().toUpperCase();
  var mode = String(sheet.getRange('B4').getValue() || 'AUTO').trim().toUpperCase();
  if (!title) {
    SpreadsheetApp.getUi().alert('Pick a title from the Title dropdown in B2.');
    return;
  }
  renderDashboard_(title, country || 'ALL', mode || 'AUTO');
}

function renderDashboard_(query, countryCode, mode) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  ensureAllSheets_(ss);
  var sheet = ss.getSheetByName(CONFIG.SHEETS.DASHBOARD);
  setupDashboardSheet_(ss, query, countryCode, mode);
  clearDashboardOutput_(sheet);

  var selectedCountry = (countryCode && countryCode !== 'ALL') ? String(countryCode).toUpperCase() : 'ALL';
  var selectedMode = String(mode || 'AUTO').toUpperCase();

  var reconAll = getDashboardRowsFromReconciled_(ss, query, 'ALL');
  var bomAll = getDashboardRowsFromBomDetails_(ss, query, 'ALL');
  var combinedCoverage = combineDashboardCoverage_(reconAll, bomAll);

  var allRows = resolveDashboardRows_(reconAll, bomAll, selectedCountry, selectedMode);
  if (!allRows.length) {
    sheet.getRange('A7').setValue('No matching rows found.').setFontWeight('bold');
    sheet.getRange('A8').setValue('Tip');
    sheet.getRange('B8').setValue('Use the Title dropdown in B2. It is built from both Reconciled_Evidence and BOM_Weekend_Title_Rows.');
    return;
  }

  var marketCodes = selectedCountry === 'ALL' ? Object.keys(groupDashboardRowsByCountry_(allRows)).sort() : [selectedCountry];
  var preferredCode = marketCodes[0];
  var shownRows = resolveDashboardRows_(reconAll, bomAll, preferredCode, selectedMode);
  if (!shownRows.length && selectedCountry === 'ALL') {
    preferredCode = Object.keys(combinedCoverage).sort(function(a,b){ return (combinedCoverage[b].bestCount||0) - (combinedCoverage[a].bestCount||0); })[0] || marketCodes[0];
    shownRows = resolveDashboardRows_(reconAll, bomAll, preferredCode, selectedMode);
  }

  shownRows.sort(function(a, b) {
    var at = dashboardSortValue_(a.period_start_date, a.period_key);
    var bt = dashboardSortValue_(b.period_start_date, b.period_key);
    if (at !== bt) return at - bt;
    return String(a.period_key || '').localeCompare(String(b.period_key || ''));
  });

  var matchedTitle = shownRows[0].title || query;
  var totalGross = shownRows.reduce(function(sum, r) { return sum + (Number(r.gross) || 0); }, 0);
  var peakRow = shownRows.slice().sort(function(a,b){ return Number(b.gross||0) - Number(a.gross||0); })[0] || null;
  var latestRow = shownRows.length ? shownRows[shownRows.length - 1] : null;
  var chosenLabel = preferredCode === 'ALL' ? 'All comparable markets' : ((CONFIG.MARKETS[preferredCode] && CONFIG.MARKETS[preferredCode].country) || preferredCode);

  var summary = [
    ['Matched Title', matchedTitle, 'Source Mode', selectedMode],
    ['Country Shown', chosenLabel, 'Records', shownRows.length],
    ['Chosen Source', latestRow ? latestRow.source_name : '', 'Currency', shownRows.length ? unique_(shownRows.map(function(r){ return r.currency; }).filter(Boolean)).join(', ') : ''],
    ['Total Period Gross', totalGross || '', 'Peak Period', peakRow ? peakRow.display_period : ''],
    ['Peak Gross', peakRow ? peakRow.gross : '', 'Latest Period', latestRow ? latestRow.display_period : ''],
    ['Latest Gross', latestRow ? latestRow.gross : '', 'Selected Country', selectedCountry]
  ];
  sheet.getRange(7,1,summary.length,4).setValues(summary);
  sheet.getRange(7,1,1,4).setFontWeight('bold').setBackground('#E5E7EB');

  var noteRow = 14;
  sheet.getRange(noteRow, 1).setValue('Dashboard Notes').setFontWeight('bold').setBackground('#FDE68A');
  sheet.getRange(noteRow + 1, 1).setValue('AUTO chooses the stronger source by market. For UAE-like markets, BOM will win when it has richer weekend history than RECON.');

  var coverageRow = noteRow + 3;
  sheet.getRange(coverageRow, 1).setValue('Available Market Coverage').setFontWeight('bold').setBackground('#DBEAFE');
  var covHeader = [['Country', 'Code', 'Best Source', 'Records', 'First Period', 'Last Period']];
  sheet.getRange(coverageRow + 1, 1, 1, covHeader[0].length).setValues(covHeader).setFontWeight('bold');
  var coverageTable = Object.keys(combinedCoverage).sort().map(function(code){
    var c = combinedCoverage[code];
    return [
      (CONFIG.MARKETS[code] && CONFIG.MARKETS[code].country) || code,
      code,
      c.bestSource,
      c.bestCount,
      c.firstPeriod || '',
      c.lastPeriod || ''
    ];
  });
  if (coverageTable.length) sheet.getRange(coverageRow + 2, 1, coverageTable.length, covHeader[0].length).setValues(coverageTable);

  var tableRow = coverageRow + 4 + Math.max(coverageTable.length, 1);
  sheet.getRange(tableRow, 1).setValue('Performance Table').setFontWeight('bold').setBackground('#D1FAE5');
  var tableHeader = [['Period', 'Start Date', 'End Date', 'Country', 'Gross', 'Currency', 'Source']];
  sheet.getRange(tableRow + 1, 1, 1, tableHeader[0].length).setValues(tableHeader).setFontWeight('bold');
  var tableValues = shownRows.map(function(r){
    return [r.display_period, r.period_start_date, r.period_end_date, r.country, r.gross, r.currency, r.source_name];
  });
  sheet.getRange(tableRow + 2, 1, tableValues.length, tableHeader[0].length).setValues(tableValues);

  var helper = shownRows.map(function(r){ return [r.display_period, Number(r.gross) || 0, r.source_name]; });
  sheet.getRange(1,10,1,3).setValues([['Period','Gross','Source']]);
  if (helper.length) sheet.getRange(2,10,helper.length,3).setValues(helper);

  var chart = sheet.newChart()
    .asLineChart()
    .addRange(sheet.getRange(1, 10, helper.length + 1, 2))
    .setPosition(7, 6, 0, 0)
    .setOption('title', matchedTitle + ' — ' + chosenLabel)
    .setOption('legend', { position: 'none' })
    .setOption('hAxis', { title: 'Period' })
    .setOption('vAxis', { title: 'Gross (' + ((latestRow && latestRow.currency) || 'Local') + ')' })
    .build();
  sheet.insertChart(chart);

  try { sheet.hideColumns(10, 3); } catch (e) {}
  if (tableValues.length) {
    sheet.getRange(tableRow + 2, 2, tableValues.length, 2).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(tableRow + 2, 5, tableValues.length, 1).setNumberFormat('#,##0.00');
  }
  autoResize_(sheet, 12);
}

function resolveDashboardRows_(reconRows, bomRows, countryCode, mode) {
  var reconBy = groupDashboardRowsByCountry_(reconRows);
  var bomBy = groupDashboardRowsByCountry_(bomRows);
  var codes = countryCode && countryCode !== 'ALL' ? [countryCode] : unique_(Object.keys(reconBy).concat(Object.keys(bomBy))).sort();
  var out = [];
  codes.forEach(function(code) {
    var rr = reconBy[code] || [];
    var bb = bomBy[code] || [];
    var chosen = [];
    if (mode === 'RECON') {
      chosen = rr;
    } else if (mode === 'BOM') {
      chosen = bb;
    } else {
      chosen = chooseBestDashboardSourceRows_(rr, bb);
    }
    out = out.concat(chosen);
  });
  return out;
}

function groupDashboardRowsByCountry_(rows) {
  var out = {};
  (rows || []).forEach(function(r){
    var code = String(r.country_code || '').toUpperCase();
    if (!code) return;
    if (!out[code]) out[code] = [];
    out[code].push(r);
  });
  return out;
}

function chooseBestDashboardSourceRows_(reconRows, bomRows) {
  reconRows = reconRows || [];
  bomRows = bomRows || [];
  if (!reconRows.length) return bomRows;
  if (!bomRows.length) return reconRows;

  var reconPeriods = unique_(reconRows.map(function(r){ return r.period_key; }).filter(Boolean)).length;
  var bomPeriods = unique_(bomRows.map(function(r){ return r.period_key; }).filter(Boolean)).length;
  var reconLatest = reconRows.slice().sort(function(a,b){ return dashboardSortValue_(b.period_start_date,b.period_key)-dashboardSortValue_(a.period_start_date,a.period_key); })[0];
  var bomLatest = bomRows.slice().sort(function(a,b){ return dashboardSortValue_(b.period_start_date,b.period_key)-dashboardSortValue_(a.period_start_date,a.period_key); })[0];

  if (bomPeriods > reconPeriods) return bomRows;
  if (reconPeriods > bomPeriods) return reconRows;
  if ((dashboardSortValue_(bomLatest && bomLatest.period_start_date, bomLatest && bomLatest.period_key)) > (dashboardSortValue_(reconLatest && reconLatest.period_start_date, reconLatest && reconLatest.period_key))) return bomRows;
  return reconRows;
}

function combineDashboardCoverage_(reconRows, bomRows) {
  var out = {};
  function absorb(rows, sourceName) {
    groupDashboardRowsByCountry_(rows || []);
    (rows || []).forEach(function(r){
      var code = String(r.country_code || '').toUpperCase();
      if (!code) return;
      if (!out[code]) out[code] = { code: code, reconRows: [], bomRows: [] };
      if (sourceName === 'RECON') out[code].reconRows.push(r);
      if (sourceName === 'BOM') out[code].bomRows.push(r);
    });
  }
  absorb(reconRows, 'RECON');
  absorb(bomRows, 'BOM');

  Object.keys(out).forEach(function(code){
    var recon = out[code].reconRows || [];
    var bom = out[code].bomRows || [];
    var best = chooseBestDashboardSourceRows_(recon, bom);
    best.sort(function(a,b){ return dashboardSortValue_(a.period_start_date,a.period_key) - dashboardSortValue_(b.period_start_date,b.period_key); });
    out[code].bestSource = best.length && best[0].source_name === 'BOXOFFICEMOJO_WEEKEND_DETAIL' ? 'BOM' : 'RECON';
    out[code].bestCount = best.length;
    out[code].firstPeriod = best.length ? best[0].display_period : '';
    out[code].lastPeriod = best.length ? best[best.length - 1].display_period : '';
  });
  return out;
}
