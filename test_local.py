"""
Local test script — run against real Gemini API with sample files.
Usage:
  export GEMINI_API_KEY="your-key"
  python test_local.py /path/to/sample/folder

The folder should contain files matching the app's upload format:
  001.webp, 001_meta.txt, 002.webp, 002_meta.txt, ..., session.log
"""
import os
import sys
import google.generativeai as genai

def main():
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Set GEMINI_API_KEY env var")
        sys.exit(1)

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.0-flash')

    # Categorize files
    image_files = {}
    meta_files = {}
    session_log = ""

    for fname in os.listdir(folder):
        fpath = os.path.join(folder, fname)
        if fname == "session.log":
            with open(fpath, "r") as f:
                session_log = f.read()
        elif fname.endswith("_meta.txt"):
            key = fname.replace("_meta.txt", "")
            with open(fpath, "r") as f:
                meta_files[key] = f.read()
        elif fname.endswith((".webp", ".png", ".jpg", ".jpeg")):
            key = os.path.splitext(fname)[0]
            image_files[key] = fpath

    sorted_keys = sorted(image_files.keys())
    print(f"Found {len(sorted_keys)} images, {len(meta_files)} meta files, session_log={'yes' if session_log else 'no'}")

    # Build prompt
    temp_uploads = []
    inputs = []
    for i, key in enumerate(sorted_keys):
        uploaded = genai.upload_file(image_files[key])
        temp_uploads.append(uploaded)
        inputs.append(f"\n--- Screen capture {i+1} ({key}) ---")
        inputs.append(uploaded)
        if key in meta_files:
            inputs.append(f"Accessibility tree for this screen:\n{meta_files[key]}")

    if session_log:
        inputs.append(f"\n--- Session activity log ---\n{session_log}")

    system_prompt = """You are an expert Android accessibility auditor. You are given sequential screen captures from a mobile app along with the corresponding Android accessibility tree for each screen.

The accessibility tree data format is:
- TXT: The text or contentDescription of the UI element
- ID: The Android resource ID (may be empty)
- CLS: The Android widget class

For each screen, analyze the following accessibility issues:

1. **Missing content descriptions**: Images, buttons, or interactive elements with no TXT or generic TXT (e.g., just "ImageView")
2. **Duplicate or ambiguous labels**: Multiple elements with the same TXT that a screen reader user cannot distinguish
3. **Missing element roles**: Interactive elements using generic classes (ViewGroup) instead of proper widgets (Button, CheckBox)
4. **Touch target issues**: Elements that appear to be interactive but lack proper accessibility labels
5. **Navigation structure**: Missing headings, landmarks, or logical grouping for screen reader navigation

Group your findings by app (identified in the CAPTURE header, e.g., com.google.android.youtube).

For each issue found, provide:
- The specific element(s) affected (quote the TXT and ID)
- The WCAG guideline violated
- A concrete fix recommendation

End with a severity summary: Critical / Major / Minor issue counts."""

    print("\nSending to Gemini...\n")
    response = model.generate_content([system_prompt] + inputs)
    print(response.text)

    # Cleanup
    for f in temp_uploads:
        try:
            f.delete()
        except Exception:
            pass

if __name__ == "__main__":
    main()
