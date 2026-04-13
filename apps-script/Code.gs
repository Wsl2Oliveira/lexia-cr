/**
 * LexIA — Google Apps Script Web App
 *
 * Receives a POST with case data, copies the template (preserving layout),
 * replaces placeholders, and saves the Google Doc in the target folder.
 *
 * Deploy: Extensions > Apps Script > Deploy > New deployment > Web app
 *   - Execute as: Me
 *   - Who has access: Anyone (or Anyone with Google account)
 */

function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    var templateId = data.templateId;
    var folderId = data.folderId;
    var docName = data.docName;
    var replacements = data.replacements;

    var folder = DriveApp.getFolderById(folderId);
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
    doc.saveAndClose();

    var result = {
      docId: docId,
      docUrl: "https://docs.google.com/document/d/" + docId + "/edit",
    };

    return ContentService.createTextOutput(JSON.stringify(result)).setMimeType(
      ContentService.MimeType.JSON
    );
  } catch (err) {
    return ContentService.createTextOutput(
      JSON.stringify({ error: err.message })
    ).setMimeType(ContentService.MimeType.JSON);
  }
}

/**
 * Replace a placeholder with text that may contain \n.
 * Google Docs replaceText() ignores newlines, so we find the placeholder,
 * split the replacement into lines, insert the first line in place, and
 * append subsequent lines as new paragraphs right after.
 */
function replaceTextWithNewlines(body, pattern, replacement) {
  var found = body.findText(pattern);
  if (!found) return;

  var element = found.getElement();
  var start = found.getStartOffset();
  var end = found.getEndOffsetInclusive();
  var text = element.asText();

  var lines = replacement.split("\n");

  text.deleteText(start, end);
  text.insertText(start, lines[0]);

  if (lines.length > 1) {
    var parent = element.getParent();
    var parentIndex = body.getChildIndex(parent);

    for (var i = lines.length - 1; i >= 1; i--) {
      var newParagraph = body.insertParagraph(parentIndex + 1, lines[i]);
      newParagraph.setAttributes(parent.getAttributes());
    }
  }
}

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Quick test — run manually from the Apps Script editor to verify
 * that template access and folder permissions work.
 */
function testDoPost() {
  var mockEvent = {
    postData: {
      contents: JSON.stringify({
        templateId: "YOUR_TEMPLATE_DOC_ID",
        folderId: "YOUR_DRIVE_FOLDER_ID",
        docName: "TEST-LexIA-AppsScript",
        replacements: {
          "{{data da elaboração deste documento}}": "10 de abril de 2026",
          "{{número do ofício}}": "TEST-001",
          "{{número do processo}}": "0000000-00.0000.0.00.0000",
          "{{Vara/Seccional}}": "Vara de Teste",
          "{{Órgão (delegacia/tribunal)}}": "Tribunal de Teste",
          "{{NOME DO CLIENTE ATINGIDO}}": "Fulano de Teste",
          "CPF (CNPJ)": "CPF",
          "{{documento do cliente atingido}}": "000.000.000-00",
          "{{macro da operação realizada}}":
            "foi realizado o bloqueio judicial de ativos financeiros no valor de R$ 100,00.",
        },
        exportPdf: false,
      }),
    },
  };

  var response = doPost(mockEvent);
  Logger.log(response.getContent());
}
