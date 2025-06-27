export function randomHttpErrorInDev(p) {
  if (process.env.NODE_ENV === "development") {
    if (Math.random() < p) {
      throw app.httpErrors.internalServerError('Random dev error for testing')
    }
  }
}