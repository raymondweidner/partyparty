import * as admin from 'firebase-admin';
import { MulticastMessage } from 'firebase-admin/messaging';
import { logger } from './logger';

/**
 * Sends messages to devices via FCM.
 * @param payload - The message payload object.
 * @param type - The message type string.
 * @param tokens - An array of FCM tokens for destinations.
 */
export const sendMessagesToDevices = async (payload: any | null, type: string, tokens: string[]) => {
  logger.info({ payload: payload, type: type, tokens: tokens }, 'Sending messages to devices');

  if (!tokens || tokens.length === 0) {
    logger.info("NO TOKENS, returning");
    return;
  }

  if (type === 'chat') {
    const { text } = payload;
    logger.info({ text }, "SENDING CHAT TEXT");
    if (text) {
      const message: MulticastMessage = {
        notification: {
          body: text,
        },
        tokens: tokens,
      };

      try {
        const response = await admin.messaging().sendEachForMulticast(message);
        logger.info({ successCount: response.successCount, failureCount: response.failureCount, responses: response.responses }, 'FCM messages sent');
        return response;
      } catch (error) {
        logger.error({ error }, 'Error sending FCM messages');
        throw error;
      }
    }
  } else if (type === 'notification') {
    logger.info("SENDING NOTIFICATION");
    const { notificationId, title, body, htmlBody, resourceType, resourceId, actionMode } = payload;
    if (body) {
      const dataPayload: any = {};
      if (notificationId) dataPayload.notificationId = String(notificationId);
      if (htmlBody) dataPayload.html_body = htmlBody;
      if (resourceType) dataPayload.resourceType = resourceType;
      if (resourceId) dataPayload.resourceId = String(resourceId);
      if (actionMode) dataPayload.actionMode = actionMode;

      const message: MulticastMessage = {
        notification: {
          title,
          body,
        },
        data: Object.keys(dataPayload).length > 0 ? dataPayload : undefined,
        tokens: tokens,
      };

      try {
        const response = await admin.messaging().sendEachForMulticast(message);
        logger.info({ successCount: response.successCount, failureCount: response.failureCount, responses: response.responses }, 'FCM notifications sent');
        return response;
      } catch (error) {
        logger.error({ error }, 'Error sending FCM notifications');
        throw error;
      }
    }
  }
};