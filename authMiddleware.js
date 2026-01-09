const admin = require('firebase-admin');

/**
 * Validates the Firebase ID token passed in the Authorization header.
 * @param {object} req The request object.
 * @param {object} res The response object.
 * @param {function} next The next middleware function.
 * @return {Promise<void>}
 */
const validateFirebaseIdToken = async (req, res, next) => {
  if (!req.headers.authorization ||
      !req.headers.authorization.startsWith('Bearer ')) {
    console.error(
        'No Firebase ID token was passed as a Bearer token in the Authorization header.',
        'Make sure you authorize your request by providing the following HTTP header:',
        'Authorization: Bearer <Firebase ID Token>',
    );
    res.status(403).send('Unauthorized');
    return;
  }

  const idToken = req.headers.authorization.split('Bearer ')[1];

  try {
    const decodedIdToken = await admin.auth().verifyIdToken(idToken);
    req.user = decodedIdToken;
    next();
    return;
  } catch (error) {
    console.error('Error while verifying Firebase ID token:', error);
    res.status(403).send('Unauthorized');
    return;
  }
};

module.exports = validateFirebaseIdToken;
