export function randomHttpErrorInDev(p) {
  if (process.env.NODE_ENV === "development") {
    if (Math.random() < p) {
      throw { status: 418, message: 'Random dev error for testing' }
    }
  }
}