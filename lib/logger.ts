import pino from 'pino';

const isProduction = process.env.NODE_ENV === 'production';

export const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  formatters: isProduction ? {
    level(label) {
      return { severity: label.toUpperCase() };
    },
  } : undefined,
  messageKey: isProduction ? 'message' : 'msg',
  transport: !isProduction
    ? {
        target: 'pino-pretty',
        options: {
          colorize: true,
          translateTime: 'SYS:standard',
        },
      }
    : undefined, // Use pure JSON in production
});
