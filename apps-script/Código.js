/**
 * LexIA — Google Apps Script Web App
 *
 * Endpoints (via POST):
 *   action: "generate"  → Copies template, fills placeholders, saves Google Doc
 *   action: "search"    → Searches Drive for ofício PDFs by process numbers
 *   action: "delete"    → Deletes files by ID (permanently moves to trash)
 *
 * Deploy: Extensions > Apps Script > Deploy > New deployment > Web app
 *   - Execute as: Me
 *   - Who has access: Anyone (or Anyone with Google account)
 */

function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    var action = data.action || "generate";

    if (action === "search") {
      return _handleSearch(data);
    }
    if (action === "delete") {
      return _handleDelete(data);
    }
    return _handleGenerate(data);
  } catch (err) {
    return ContentService.createTextOutput(
      JSON.stringify({ error: err.message })
    ).setMimeType(ContentService.MimeType.JSON);
  }
}

// ─────────────────────────────────────────────────────────────────
// Action: generate — copy template + fill placeholders
// ─────────────────────────────────────────────────────────────────

function _handleGenerate(data) {
  var templateId = data.templateId;
  var folderId = data.folderId;
  var docName = data.docName;
  var replacements = data.replacements;

  var parentFolder = DriveApp.getFolderById(folderId);
  var folder = parentFolder;
  if (data.subfolderName) {
    folder = parentFolder.createFolder(data.subfolderName);
  }

  var copy = DriveApp.getFileById(templateId).makeCopy(docName, folder);
  var docId = copy.getId();

  var doc = DocumentApp.openById(docId);
  var body = doc.getBody();

  for (var findText in replacements) {
    var value = replacements[findText];
    if (value.indexOf("\n") !== -1) {
      replaceTextWithNewlines(body, escapeRegex(findText), value);
    } else {
      body.replaceText(escapeRegex(findText), value);
    }
  }

  var boldTexts = data.boldTexts || [];
  for (var b = 0; b < boldTexts.length; b++) {
    _applyBoldToText(body, boldTexts[b]);
  }

  doc.saveAndClose();

  var result = {
    docId: docId,
    docUrl: "https://docs.google.com/document/d/" + docId + "/edit",
    folderId: folder.getId(),
    folderUrl: "https://drive.google.com/drive/folders/" + folder.getId(),
  };

  return ContentService.createTextOutput(JSON.stringify(result)).setMimeType(
    ContentService.MimeType.JSON
  );
}

// ─────────────────────────────────────────────────────────────────
// Action: search — find ofício PDFs by process number
// ─────────────────────────────────────────────────────────────────

function _handleSearch(data) {
  var processes = data.processes || [];
  var folderIds = data.folderIds || [];

  var results = {};

  for (var i = 0; i < processes.length; i++) {
    var processo = processes[i];
    results[processo] = _searchOficio(processo, folderIds);
  }

  return ContentService.createTextOutput(JSON.stringify(results)).setMimeType(
    ContentService.MimeType.JSON
  );
}

/**
 * Search for a PDF matching the process number.
 * Strategy:
 *   1. Search in specified folders (recursive)
 *   2. If not found, search entire Drive
 * Returns object with id, name, url, folder or null.
 */
function _searchOficio(processNumber, folderIds) {
  // 1) Search in specific folders first
  for (var f = 0; f < folderIds.length; f++) {
    try {
      var folder = DriveApp.getFolderById(folderIds[f]);
      var found = _searchInFolder(folder, processNumber);
      if (found) return found;
    } catch (e) {
      // folder not accessible, skip
    }
  }

  // 2) Global Drive search
  var query =
    "title contains '" + processNumber + "' and mimeType = 'application/pdf' and trashed = false";
  var files = DriveApp.searchFiles(query);

  while (files.hasNext()) {
    var file = files.next();
    var name = file.getName();
    if (name.indexOf(processNumber) !== -1 && !name.startsWith("CR-")) {
      return _fileInfo(file);
    }
  }

  // 3) Try without PDF filter (could be other formats)
  query = "title contains '" + processNumber + "' and trashed = false";
  files = DriveApp.searchFiles(query);

  while (files.hasNext()) {
    var file = files.next();
    var name = file.getName();
    if (name.indexOf(processNumber) !== -1 && !name.startsWith("CR-")) {
      return _fileInfo(file);
    }
  }

  return null;
}

function _searchInFolder(folder, processNumber) {
  // Search files in this folder
  var files = folder.getFiles();
  while (files.hasNext()) {
    var file = files.next();
    var name = file.getName();
    if (name.indexOf(processNumber) !== -1 && !name.startsWith("CR-")) {
      return _fileInfo(file);
    }
  }

  // Recurse into subfolders
  var subfolders = folder.getFolders();
  while (subfolders.hasNext()) {
    var sub = subfolders.next();
    var found = _searchInFolder(sub, processNumber);
    if (found) return found;
  }

  return null;
}

function _fileInfo(file) {
  var parents = file.getParents();
  var folderName = "";
  if (parents.hasNext()) {
    folderName = parents.next().getName();
  }

  return {
    id: file.getId(),
    name: file.getName(),
    mimeType: file.getMimeType(),
    url: "https://drive.google.com/file/d/" + file.getId() + "/view",
    folder: folderName,
    lastUpdated: file.getLastUpdated().toISOString(),
  };
}

// ─────────────────────────────────────────────────────────────────
// Action: delete — trash files by ID
// ─────────────────────────────────────────────────────────────────

function _handleDelete(data) {
  var fileIds = data.fileIds || [];
  var results = [];

  for (var i = 0; i < fileIds.length; i++) {
    try {
      var file = DriveApp.getFileById(fileIds[i]);
      var name = file.getName();
      file.setTrashed(true);
      results.push({ id: fileIds[i], name: name, status: "trashed" });
    } catch (err) {
      results.push({ id: fileIds[i], status: "error", message: err.message });
    }
  }

  return ContentService.createTextOutput(JSON.stringify({ results: results })).setMimeType(
    ContentService.MimeType.JSON
  );
}

// ─────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────

function replaceTextWithNewlines(body, pattern, replacement) {
  var found = body.findText(pattern);
  if (!found) return;

  var element = found.getElement();
  var start = found.getStartOffset();
  var end = found.getEndOffsetInclusive();
  var text = element.asText();
  var parent = element.getParent();
  var parentIndex = body.getChildIndex(parent);

  // Captura formatação do parágrafo original do template
  var paraStyle = {
    indentFirst: parent.getIndentFirstLine(),
    indentStart: parent.getIndentStart(),
    indentEnd: parent.getIndentEnd(),
    alignment: parent.getAlignment(),
    lineSpacing: parent.getLineSpacing(),
  };

  // Detecta espaçamento entre parágrafos a partir do próximo parágrafo no template
  // (ex: "Ressaltamos..." tem spacingBefore que define o gap visual)
  var refSpacing = 6; // fallback 6pt
  var totalChildren = body.getNumChildren();
  if (parentIndex + 1 < totalChildren) {
    var nextChild = body.getChild(parentIndex + 1);
    if (nextChild.getType() === DocumentApp.ElementType.PARAGRAPH) {
      var nextSpacing = nextChild.asParagraph().getSpacingBefore();
      if (nextSpacing != null && nextSpacing > 0) {
        refSpacing = nextSpacing;
      }
    }
  }
  // Se o próprio parágrafo tem spacingAfter, usar como referência também
  var parentAfter = parent.getSpacingAfter();
  if (parentAfter != null && parentAfter > refSpacing) {
    refSpacing = parentAfter;
  }

  paraStyle.spacingBefore = refSpacing;
  paraStyle.spacingAfter = parentAfter || 0;

  // Separa por \n\n (quebra de parágrafo) — cada bloco vira um parágrafo próprio
  var paragraphs = replacement.split("\n\n");

  // Primeiro bloco fica no parágrafo original (onde estava o placeholder)
  text.deleteText(start, end);
  text.insertText(start, paragraphs[0]);

  // Demais blocos viram parágrafos novos com Verdana 10 e formatação do template
  if (paragraphs.length > 1) {
    for (var i = paragraphs.length - 1; i >= 1; i--) {
      var content = paragraphs[i].replace(/\n/g, " ").trim();
      if (content === "") continue;

      var newParagraph = body.insertParagraph(parentIndex + 1, content);
      _applyParaStyle(newParagraph, paraStyle);
    }
  }
}

function _applyParaStyle(paragraph, style) {
  // Fonte padrão: Verdana 10
  var textStyle = {};
  textStyle[DocumentApp.Attribute.FONT_FAMILY] = "Verdana";
  textStyle[DocumentApp.Attribute.FONT_SIZE] = 10;
  textStyle[DocumentApp.Attribute.BOLD] = false;
  textStyle[DocumentApp.Attribute.ITALIC] = false;
  paragraph.setAttributes(textStyle);

  // Atributos de parágrafo
  if (style.indentFirst != null) paragraph.setIndentFirstLine(style.indentFirst);
  if (style.indentStart != null) paragraph.setIndentStart(style.indentStart);
  if (style.indentEnd != null) paragraph.setIndentEnd(style.indentEnd);
  if (style.alignment != null) paragraph.setAlignment(style.alignment);
  if (style.lineSpacing != null) paragraph.setLineSpacing(style.lineSpacing);
  paragraph.setSpacingBefore(style.spacingBefore || 6);
  paragraph.setSpacingAfter(style.spacingAfter || 0);
}

function _applyBoldToText(body, searchText) {
  var found = body.findText(escapeRegex(searchText));
  while (found) {
    var element = found.getElement().asText();
    var start = found.getStartOffset();
    var end = found.getEndOffsetInclusive();
    element.setBold(start, end, true);
    found = body.findText(escapeRegex(searchText), found);
  }
}

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// ─────────────────────────────────────────────────────────────────
// Test functions (run from Apps Script editor)
// ─────────────────────────────────────────────────────────────────

function testSearch() {
  var mockEvent = {
    postData: {
      contents: JSON.stringify({
        action: "search",
        processes: [
          "0000242-08.2024.8.26.0619",
          "0000269-03.2022.5.11.0013",
          "0000483-21.2019.8.26.0210",
          "0000637-19.2019.5.19.0005",
          "0000990-21.2023.8.16.0080",
        ],
        folderIds: [
          "YOUR_DRIVE_FOLDER_ID",
        ],
      }),
    },
  };

  var response = doPost(mockEvent);
  Logger.log(response.getContent());
}

function testGenerate() {
  var mockEvent = {
    postData: {
      contents: JSON.stringify({
        action: "generate",
        templateId: "YOUR_TEMPLATE_DOC_ID",
        folderId: "YOUR_DRIVE_FOLDER_ID",
        docName: "TEST-LexIA-AppsScript",
        replacements: {
          "{{data da elaboração deste documento}}": "13 de abril de 2026",
          "{{número do ofício}}": "TEST-001",
          "{{número do processo}}": "0000000-00.0000.0.00.0000",
          "{{Vara/Seccional}}": "Vara de Teste",
          "{{Órgão (delegacia/tribunal)}}": "Tribunal de Teste",
          "{{NOME DO CLIENTE ATINGIDO}}": "Fulano de Teste",
          "CPF (CNPJ)": "CPF",
          "{{documento do cliente atingido}}": "000.000.000-00",
          "{{macro da operação realizada}}": "teste de macro.",
        },
      }),
    },
  };

  var response = doPost(mockEvent);
  Logger.log(response.getContent());
}
