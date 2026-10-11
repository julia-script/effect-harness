---
'effect-harness': patch
---

Round the strict portable JPEG decoder's secondary resolution allowance outward so valid baseline and progressive images pass at their exact source pixel budget, while retaining the exact header pixel guard and independent decoder memory cap.
