import * as admin from 'firebase-admin';
import { MulticastMessage } from 'firebase-admin/messaging';

/**
 * Sends messages to devices via FCM.
 * @param payload - The message payload object.
 * @param type - The message type string.
 * @param tokens - An array of FCM tokens for destinations.
 */
export const sendMessagesToDevices = async (payload: any | null, type: string, tokens: string[]) => {
  if (!tokens || tokens.length === 0) {
    return;
  }

  if (type === 'chat') {
    const { text } = payload;
    if (text) {
      const message: MulticastMessage = {
        notification: {
          body: text,
        },
        tokens: tokens,
      };

      try {
        const response = await admin.messaging().sendEachForMulticast(message);
        console.log(`${response.successCount} messages were sent successfully`);
        return response;
      } catch (error) {
        console.error('Error sending FCM messages:', error);
        throw error;
      }
    }
  }
};