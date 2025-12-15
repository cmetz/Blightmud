use std::{io::Read, io::Write, sync::Arc, sync::Mutex, thread, time::Duration};

// --- ReadWrapper mit "Polite Polling" ---

struct ReadWrapper<T> {
    inner: Arc<Mutex<T>>,
}

impl<T: Read> Read for ReadWrapper<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            // 1. Lock holen
            let mut stream = self.inner.lock().map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("Mutex poisoned: {}", e))
            })?;

            // 2. Versuchen zu lesen
            match stream.read(buf) {
                Ok(n) => return Ok(n), // Erfolg!
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Keine Daten da. WICHTIG: Lock fallen lassen!
                    drop(stream);
                    
                    // Kurz schlafen (1ms), um CPU nicht zu grillen und dem Writer eine Chance zu geben
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(e) => return Err(e), // Echter Fehler
            }
        }
    }
}

// --- WriteWrapper mit "Polite Polling" ---
// Auch Schreiben kann blockieren, wenn der Puffer voll ist.

struct WriteWrapper<T> {
    inner: Arc<Mutex<T>>,
}

impl<T: Write> Write for WriteWrapper<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        loop {
            let mut stream = self.inner.lock().map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("Mutex poisoned: {}", e))
            })?;

            match stream.write(buf) {
                Ok(n) => return Ok(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    drop(stream);
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        loop {
            let mut stream = self.inner.lock().map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("Mutex poisoned: {}", e))
            })?;

            match stream.flush() {
                Ok(()) => return Ok(()),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    drop(stream);
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        // Standard write_all nutzt write(), aber da wir den Lock managen müssen,
        // ist es sicherer, das hier auch zu loopen, falls write_all "WouldBlock" zurückgeben könnte
        // (was es normalerweise nicht tut, es ruft write in loop auf). 
        // Wir können hier einfach an write() delegieren, da unsere write() impl schon den Loop hat.
        // Aber write_all in std impl hält keinen internen State, also ist default impl okay.
        // Der Einfachheit halber nutzen wir hier einen Loop um write, falls nötig.
        let mut buf = buf;
        while !buf.is_empty() {
            match self.write(buf) {
                Ok(0) => return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "failed to write whole buffer")),
                Ok(n) => buf = &buf[n..],
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

// --- RwStream (unverändert zur letzten Version) ---

pub struct RwStream<T> {
    inner: Arc<Mutex<T>>,
    pub input_stream: Arc<Mutex<dyn Read + Send>>,
    pub output_stream: Arc<Mutex<dyn Write + Send>>,
}

impl<T: Read + Write + Send + 'static> RwStream<T> {
    pub fn new(stream: T) -> Self {
        let shared_stream = Arc::new(Mutex::new(stream));

        let read_wrapper = ReadWrapper {
            inner: shared_stream.clone(),
        };
        
        let write_wrapper = WriteWrapper {
            inner: shared_stream.clone(),
        };

        let input_stream = Arc::new(Mutex::new(read_wrapper));
        let output_stream = Arc::new(Mutex::new(write_wrapper));

        Self {
            inner: shared_stream,
            input_stream,
            output_stream,
        }
    }

    pub fn inner(&self) -> std::sync::MutexGuard<'_, T> {
        self.inner.lock().unwrap()
    }

    pub fn inner_mut(&self) -> std::sync::MutexGuard<'_, T> {
        self.inner()
    }
}

impl<T> Clone for RwStream<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            input_stream: self.input_stream.clone(),
            output_stream: self.output_stream.clone(),
        }
    }
}
