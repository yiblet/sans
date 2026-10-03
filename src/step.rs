/// A step that yields an output or completes with a final result.
///
/// [`Sans::next`](crate::Sans::next) returns this enum.
/// [`InitSans::init`](crate::InitSans::init) includes a continuation with its output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Step<Y, D> {
    /// An output; the coroutine can accept more input.
    Yielded(Y),
    /// The final result; stop calling the coroutine.
    Complete(D),
}

impl<Y, D> Step<Y, D> {
    /// Returns `true` if the step is `Yielded`.
    #[inline]
    pub const fn is_yielded(&self) -> bool {
        matches!(self, Step::Yielded(_))
    }

    /// Returns `true` if the step is `Complete`.
    #[inline]
    pub const fn is_complete(&self) -> bool {
        matches!(self, Step::Complete(_))
    }

    /// Take the yielded output, or `None` if complete.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Yielded(42);
    /// assert_eq!(x.yielded_value(), Some(42));
    ///
    /// let y: Step<i32, &str> = Step::Complete("complete");
    /// assert_eq!(y.yielded_value(), None);
    /// ```
    #[inline]
    pub fn yielded_value(self) -> Option<Y> {
        match self {
            Step::Yielded(y) => Some(y),
            Step::Complete(_) => None,
        }
    }

    /// Take the final result, or `None` if yielded.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Complete("complete");
    /// assert_eq!(x.complete_value(), Some("complete"));
    ///
    /// let y: Step<i32, &str> = Step::Yielded(42);
    /// assert_eq!(y.complete_value(), None);
    /// ```
    #[inline]
    pub fn complete_value(self) -> Option<D> {
        match self {
            Step::Yielded(_) => None,
            Step::Complete(d) => Some(d),
        }
    }

    /// Convert the final result, leaving yielded outputs unchanged.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, i32> = Step::Complete(5);
    /// assert_eq!(x.map_complete(|v| v * 2), Step::Complete(10));
    ///
    /// let y: Step<i32, i32> = Step::Yielded(3);
    /// assert_eq!(y.map_complete(|v| v * 2), Step::Yielded(3));
    /// ```
    #[inline]
    pub fn map_complete<D2, F>(self, f: F) -> Step<Y, D2>
    where
        F: FnOnce(D) -> D2,
    {
        match self {
            Step::Yielded(y) => Step::Yielded(y),
            Step::Complete(d) => Step::Complete(f(d)),
        }
    }

    /// Convert the yielded output, leaving final results unchanged.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Yielded(42);
    /// assert_eq!(x.map_yielded(|v| v * 2), Step::Yielded(84));
    ///
    /// let y: Step<i32, &str> = Step::Complete("complete");
    /// assert_eq!(y.map_yielded(|v: i32| v * 2), Step::Complete("complete"));
    /// ```
    #[inline]
    pub fn map_yielded<Y2, F>(self, f: F) -> Step<Y2, D>
    where
        F: FnOnce(Y) -> Y2,
    {
        match self {
            Step::Yielded(y) => Step::Yielded(f(y)),
            Step::Complete(d) => Step::Complete(d),
        }
    }

    /// Convert the value with the function for its variant.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, i32> = Step::Yielded(42);
    /// assert_eq!(x.map(|y| y * 2, |d| d + 1), Step::Yielded(84));
    ///
    /// let y: Step<i32, i32> = Step::Complete(10);
    /// assert_eq!(y.map(|y| y * 2, |d| d + 1), Step::Complete(11));
    /// ```
    #[inline]
    pub fn map<Y2, D2, FY, FD>(self, fy: FY, fd: FD) -> Step<Y2, D2>
    where
        FY: FnOnce(Y) -> Y2,
        FD: FnOnce(D) -> D2,
    {
        match self {
            Step::Yielded(y) => Step::Yielded(fy(y)),
            Step::Complete(d) => Step::Complete(fd(d)),
        }
    }

    /// Returns the yielded value or a default.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Yielded(42);
    /// assert_eq!(x.yielded_or(0), 42);
    ///
    /// let y: Step<i32, &str> = Step::Complete("complete");
    /// assert_eq!(y.yielded_or(0), 0);
    /// ```
    #[inline]
    pub fn yielded_or(self, default: Y) -> Y {
        match self {
            Step::Yielded(y) => y,
            Step::Complete(_) => default,
        }
    }

    /// Return the yielded output, or call `f` if complete.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Yielded(42);
    /// assert_eq!(x.yielded_or_else(|| 0), 42);
    ///
    /// let y: Step<i32, &str> = Step::Complete("complete");
    /// assert_eq!(y.yielded_or_else(|| 0), 0);
    /// ```
    #[inline]
    pub fn yielded_or_else<F>(self, f: F) -> Y
    where
        F: FnOnce() -> Y,
    {
        match self {
            Step::Yielded(y) => y,
            Step::Complete(_) => f(),
        }
    }

    /// Return the final result or a default.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Complete("complete");
    /// assert_eq!(x.complete_or("default"), "complete");
    ///
    /// let y: Step<i32, &str> = Step::Yielded(42);
    /// assert_eq!(y.complete_or("default"), "default");
    /// ```
    #[inline]
    pub fn complete_or(self, default: D) -> D {
        match self {
            Step::Yielded(_) => default,
            Step::Complete(d) => d,
        }
    }

    /// Return the final result, or call `f` if yielded.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Complete("complete");
    /// assert_eq!(x.complete_or_else(|| "default"), "complete");
    ///
    /// let y: Step<i32, &str> = Step::Yielded(42);
    /// assert_eq!(y.complete_or_else(|| "default"), "default");
    /// ```
    #[inline]
    pub fn complete_or_else<F>(self, f: F) -> D
    where
        F: FnOnce() -> D,
    {
        match self {
            Step::Yielded(_) => f(),
            Step::Complete(d) => d,
        }
    }

    /// Borrow the contained value, keeping its variant.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, String> = Step::Yielded(42);
    /// assert_eq!(x.as_ref(), Step::Yielded(&42));
    ///
    /// let y: Step<i32, String> = Step::Complete("complete".to_string());
    /// assert_eq!(y.as_ref(), Step::Complete(&"complete".to_string()));
    /// ```
    #[inline]
    pub const fn as_ref(&self) -> Step<&Y, &D> {
        match self {
            Step::Yielded(y) => Step::Yielded(y),
            Step::Complete(d) => Step::Complete(d),
        }
    }

    /// Mutably borrow the contained value, keeping its variant.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let mut x: Step<i32, String> = Step::Yielded(42);
    /// if let Step::Yielded(y) = x.as_mut() {
    ///     *y = 100;
    /// }
    /// assert_eq!(x, Step::Yielded(100));
    /// ```
    #[inline]
    pub fn as_mut(&mut self) -> Step<&mut Y, &mut D> {
        match self {
            Step::Yielded(y) => Step::Yielded(y),
            Step::Complete(d) => Step::Complete(d),
        }
    }

    /// Swap `Yielded` and `Complete`, keeping the contained value.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Yielded(42);
    /// assert_eq!(x.flip(), Step::Complete(42));
    ///
    /// let y: Step<i32, &str> = Step::Complete("complete");
    /// assert_eq!(y.flip(), Step::Yielded("complete"));
    /// ```
    #[inline]
    pub fn flip(self) -> Step<D, Y> {
        match self {
            Step::Yielded(y) => Step::Complete(y),
            Step::Complete(d) => Step::Yielded(d),
        }
    }

    /// Return whether the yielded output equals `y`.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Yielded(42);
    /// assert!(x.contains_yielded(&42));
    /// assert!(!x.contains_yielded(&100));
    ///
    /// let y: Step<i32, &str> = Step::Complete("complete");
    /// assert!(!y.contains_yielded(&42));
    /// ```
    #[inline]
    pub fn contains_yielded<U>(&self, y: &U) -> bool
    where
        U: PartialEq<Y>,
    {
        matches!(self, Step::Yielded(v) if y == v)
    }

    /// Return whether the final result equals `d`.
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Complete("complete");
    /// assert!(x.contains_complete(&"complete"));
    /// assert!(!x.contains_complete(&"other"));
    ///
    /// let y: Step<i32, &str> = Step::Yielded(42);
    /// assert!(!y.contains_complete(&"complete"));
    /// ```
    #[inline]
    pub fn contains_complete<U>(&self, d: &U) -> bool
    where
        U: PartialEq<D>,
    {
        matches!(self, Step::Complete(v) if d == v)
    }

    /// Take the yielded output.
    ///
    /// # Panics
    ///
    /// Panics with `msg` if complete.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Yielded(42);
    /// assert_eq!(x.expect_yielded("was complete"), 42);
    /// ```
    #[inline]
    pub fn expect_yielded(self, msg: &str) -> Y {
        match self {
            Step::Yielded(y) => y,
            Step::Complete(_) => panic!("{}", msg),
        }
    }

    /// Take the final result.
    ///
    /// # Panics
    ///
    /// Panics with `msg` if yielded.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Complete("complete");
    /// assert_eq!(x.expect_complete("was yielding"), "complete");
    /// ```
    #[inline]
    pub fn expect_complete(self, msg: &str) -> D {
        match self {
            Step::Yielded(_) => panic!("{}", msg),
            Step::Complete(d) => d,
        }
    }

    /// Take the yielded output.
    ///
    /// # Panics
    ///
    /// Panics if complete.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Yielded(42);
    /// assert_eq!(x.unwrap_yielded(), 42);
    /// ```
    #[inline]
    pub fn unwrap_yielded(self) -> Y {
        match self {
            Step::Yielded(y) => y,
            Step::Complete(_) => panic!("called `Step::unwrap_yielded()` on a `Complete` value"),
        }
    }

    /// Take the final result.
    ///
    /// # Panics
    ///
    /// Panics if yielded.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sans::Step;
    ///
    /// let x: Step<i32, &str> = Step::Complete("complete");
    /// assert_eq!(x.unwrap_complete(), "complete");
    /// ```
    #[inline]
    pub fn unwrap_complete(self) -> D {
        match self {
            Step::Yielded(_) => panic!("called `Step::unwrap_complete()` on a `Yielded` value"),
            Step::Complete(d) => d,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_yielded_and_is_complete() {
        let y: Step<i32, &str> = Step::Yielded(42);
        let d: Step<i32, &str> = Step::Complete("complete");

        assert!(y.is_yielded());
        assert!(!y.is_complete());
        assert!(d.is_complete());
        assert!(!d.is_yielded());
    }

    #[test]
    fn test_yielded_value_and_complete_value() {
        let y: Step<i32, &str> = Step::Yielded(42);
        let d: Step<i32, &str> = Step::Complete("complete");

        assert_eq!(y.yielded_value(), Some(42));
        assert_eq!(y.complete_value(), None);
        assert_eq!(d.yielded_value(), None);
        assert_eq!(d.complete_value(), Some("complete"));
    }

    #[test]
    fn test_map_complete() {
        let y: Step<i32, i32> = Step::Yielded(42);
        let d: Step<i32, i32> = Step::Complete(10);

        assert_eq!(y.map_complete(|x| x * 2), Step::Yielded(42));
        assert_eq!(d.map_complete(|x| x * 2), Step::Complete(20));
    }

    #[test]
    fn test_map_yielded() {
        let y: Step<i32, &str> = Step::Yielded(42);
        let d: Step<i32, &str> = Step::Complete("complete");

        assert_eq!(y.map_yielded(|x| x * 2), Step::Yielded(84));
        assert_eq!(d.map_yielded(|x: i32| x * 2), Step::Complete("complete"));
    }

    #[test]
    fn test_map() {
        let y: Step<i32, i32> = Step::Yielded(42);
        let d: Step<i32, i32> = Step::Complete(10);

        assert_eq!(y.map(|x| x * 2, |x| x + 1), Step::Yielded(84));
        assert_eq!(d.map(|x| x * 2, |x| x + 1), Step::Complete(11));
    }

    #[test]
    fn test_yielded_or_and_complete_or() {
        let y: Step<i32, i32> = Step::Yielded(42);
        let d: Step<i32, i32> = Step::Complete(10);

        assert_eq!(y.yielded_or(0), 42);
        assert_eq!(d.yielded_or(0), 0);
        assert_eq!(y.complete_or(0), 0);
        assert_eq!(d.complete_or(0), 10);
    }

    #[test]
    fn test_yielded_or_else_and_complete_or_else() {
        let y: Step<i32, i32> = Step::Yielded(42);
        let d: Step<i32, i32> = Step::Complete(10);

        assert_eq!(y.yielded_or_else(|| 0), 42);
        assert_eq!(d.yielded_or_else(|| 0), 0);
        assert_eq!(y.complete_or_else(|| 0), 0);
        assert_eq!(d.complete_or_else(|| 0), 10);
    }

    #[test]
    fn test_as_ref() {
        let y: Step<i32, String> = Step::Yielded(42);
        let d: Step<i32, String> = Step::Complete("complete".to_string());

        assert_eq!(y.as_ref(), Step::Yielded(&42));
        assert_eq!(d.as_ref(), Step::Complete(&"complete".to_string()));
    }

    #[test]
    fn test_as_mut() {
        let mut y: Step<i32, String> = Step::Yielded(42);
        if let Step::Yielded(v) = y.as_mut() {
            *v = 100;
        }
        assert_eq!(y, Step::Yielded(100));

        let mut d: Step<i32, String> = Step::Complete("complete".to_string());
        if let Step::Complete(v) = d.as_mut() {
            *v = "modified".to_string();
        }
        assert_eq!(d, Step::Complete("modified".to_string()));
    }

    #[test]
    fn test_flip() {
        let y: Step<i32, &str> = Step::Yielded(42);
        let d: Step<i32, &str> = Step::Complete("complete");

        assert_eq!(y.flip(), Step::Complete(42));
        assert_eq!(d.flip(), Step::Yielded("complete"));
    }

    #[test]
    fn test_contains() {
        let y: Step<i32, &str> = Step::Yielded(42);
        let d: Step<i32, &str> = Step::Complete("complete");

        assert!(y.contains_yielded(&42));
        assert!(!y.contains_yielded(&100));
        assert!(!y.contains_complete(&"complete"));

        assert!(d.contains_complete(&"complete"));
        assert!(!d.contains_complete(&"other"));
        assert!(!d.contains_yielded(&42));
    }

    #[test]
    fn test_expect_yielded() {
        let y: Step<i32, &str> = Step::Yielded(42);
        assert_eq!(y.expect_yielded("should be yielded"), 42);
    }

    #[test]
    #[should_panic(expected = "should be yielded")]
    fn test_expect_yielded_panics() {
        let d: Step<i32, &str> = Step::Complete("complete");
        d.expect_yielded("should be yielded");
    }

    #[test]
    fn test_expect_complete() {
        let d: Step<i32, &str> = Step::Complete("complete");
        assert_eq!(d.expect_complete("should be complete"), "complete");
    }

    #[test]
    #[should_panic(expected = "should be complete")]
    fn test_expect_complete_panics() {
        let y: Step<i32, &str> = Step::Yielded(42);
        y.expect_complete("should be complete");
    }

    #[test]
    fn test_unwrap_yielded() {
        let y: Step<i32, &str> = Step::Yielded(42);
        assert_eq!(y.unwrap_yielded(), 42);
    }

    #[test]
    #[should_panic(expected = "called `Step::unwrap_yielded()` on a `Complete` value")]
    fn test_unwrap_yielded_panics() {
        let d: Step<i32, &str> = Step::Complete("complete");
        d.unwrap_yielded();
    }

    #[test]
    fn test_unwrap_complete() {
        let d: Step<i32, &str> = Step::Complete("complete");
        assert_eq!(d.unwrap_complete(), "complete");
    }

    #[test]
    #[should_panic(expected = "called `Step::unwrap_complete()` on a `Yielded` value")]
    fn test_unwrap_complete_panics() {
        let y: Step<i32, &str> = Step::Yielded(42);
        y.unwrap_complete();
    }
}
