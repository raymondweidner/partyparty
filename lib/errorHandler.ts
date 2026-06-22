import { Response } from 'express';
import { logger } from './logger';

/**
 * Inspects a database or generic error and returns an appropriate REST response.
 */
export function handleSqlErrorForRest(err: any, res: Response) {
  logger.error({ err }, 'SQL Error');
  
  if (err.code) {
    switch (err.code) {
      case '23505': // unique_violation
        return res.status(409).json({
          error: 'Conflict: Resource already exists.',
          detail: err.detail || err.message
        });
      case '23503': // foreign_key_violation
        return res.status(400).json({
          error: 'Bad Request: Invalid reference.',
          detail: err.detail || err.message
        });
      case '22P02': // invalid_text_representation
        return res.status(400).json({
          error: 'Bad Request: Invalid format.',
          detail: err.message
        });
      case '23502': // not_null_violation
        return res.status(400).json({
          error: 'Bad Request: Missing required field.',
          column: err.column,
          detail: err.message
        });
      case '42703': // undefined_column
        return res.status(400).json({
          error: 'Bad Request: Invalid column referenced.',
          detail: err.message
        });
      default:
        return res.status(500).json({
          error: 'Internal Server Error: Database operation failed.',
          code: err.code
        });
    }
  }

  // Fallbacks for custom generic errors from the data layer
  if (err.message === 'No data provided to create record' || err.message === 'No data provided to update record') {
    return res.status(400).json({ error: err.message });
  }

  return res.status(500).json({ error: err.message || 'Internal Server Error' });
}
