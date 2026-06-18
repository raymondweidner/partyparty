export const config = {
  email: {
    host: process.env.SMTP_HOST || 'smtp.gmail.com',
    port: parseInt(process.env.SMTP_PORT || '587', 10),
    auth: {
      user: process.env.SMTP_USER || 'raymond.weidner@gmail.com',
      pass: process.env.SMTP_PASS || 'yaojihqcraepqdai',
    },
  },
  app: {
    url: process.env.APP_URL || 'http://localhost:8081',
  },
};