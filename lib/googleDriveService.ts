import { google, drive_v3 } from 'googleapis';
import { logger } from './logger';
import { Readable } from 'stream';

export function getDriveClient(refreshToken: string): drive_v3.Drive {
  const oauth2Client = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    process.env.GOOGLE_REDIRECT_URI
  );

  oauth2Client.setCredentials({
    refresh_token: refreshToken,
  });

  return google.drive({ version: 'v3', auth: oauth2Client });
}

export async function createFolder(
  drive: drive_v3.Drive,
  folderName: string,
  parentFolderId?: string
): Promise<string | null> {
  try {
    // Check if folder already exists
    // Escape single quotes in folderName for the query string
    const escapedFolderName = folderName.replace(/'/g, "\\'");
    let query = `mimeType='application/vnd.google-apps.folder' and name='${escapedFolderName}' and trashed=false`;
    if (parentFolderId) {
      query += ` and '${parentFolderId}' in parents`;
    } else {
      query += ` and 'root' in parents`;
    }

    const res = await drive.files.list({
      q: query,
      fields: 'files(id, name)',
      spaces: 'drive',
    });

    if (res.data.files && res.data.files.length > 0) {
      logger.info({ folderName, folderId: res.data.files[0].id }, 'Google Drive folder already exists');
      return res.data.files[0].id!;
    }

    // Create folder
    const fileMetadata: any = {
      name: folderName,
      mimeType: 'application/vnd.google-apps.folder',
    };
    if (parentFolderId) {
      fileMetadata.parents = [parentFolderId];
    }

    const folder = await drive.files.create({
      requestBody: fileMetadata,
      fields: 'id',
    });

    logger.info({ folderName, folderId: folder.data.id }, 'Google Drive folder created');
    return folder.data.id!;
  } catch (err) {
    logger.error({ err, folderName, parentFolderId }, 'Error creating Google Drive folder');
    throw err;
  }
}

export async function uploadFileStream(
  drive: drive_v3.Drive,
  filename: string,
  mimeType: string,
  stream: Readable,
  parentFolderId: string
): Promise<string | null> {
  try {
    const fileMetadata = {
      name: filename,
      parents: [parentFolderId],
    };
    const media = {
      mimeType: mimeType,
      body: stream,
    };

    const res = await drive.files.create({
      requestBody: fileMetadata,
      media: media,
      fields: 'id, webViewLink, webContentLink',
    });

    // Make the uploaded file viewable by anyone with the link
    if (res.data.id) {
      try {
        await drive.permissions.create({
          fileId: res.data.id,
          requestBody: {
            role: 'reader',
            type: 'anyone',
          },
        });
        logger.info({ fileId: res.data.id, url: res.data.webViewLink }, 'File uploaded and permissions set');
      } catch (permErr) {
        logger.error({ permErr, fileId: res.data.id }, 'File uploaded but could not set public permissions');
      }
      return res.data.id;
    }
    return null;
  } catch (err) {
    logger.error({ err, filename }, 'Error uploading file stream to Drive');
    throw err;
  }
}

export async function deleteFile(
  drive: drive_v3.Drive,
  fileId: string
): Promise<void> {
  try {
    await drive.files.delete({ fileId });
    logger.info({ fileId }, 'Google Drive file deleted successfully');
  } catch (err) {
    logger.error({ err, fileId }, 'Error deleting file from Google Drive');
    // We don't throw because if it fails, it's just an orphaned file
  }
}
