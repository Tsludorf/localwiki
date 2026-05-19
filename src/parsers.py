#!/usr/bin/env python3
"""Parser framework for warlock_ingester."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Tuple, Dict, Any, List
import html
import json
import logging
import subprocess
import time
import os
import urllib.request
import urllib.parse
import re

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    def __init__(self, parser_name: str):
        self.parser_name = parser_name

    @abstractmethod
    def parse(self, source_item) -> Tuple[str, Dict[str, Any]]:
        pass

    def parse_documents(self, source_item) -> List[Dict[str, Any]]:
        text, metadata = self.parse(source_item)
        return [{
            "text": text,
            "metadata": metadata,
            "title": Path(source_item.uri).name,
            "source_uri": source_item.uri,
        }]


class TextParser(BaseParser):
    def __init__(self):
        super().__init__("text")

    def parse(self, source_item) -> Tuple[str, Dict[str, Any]]:
        try:
            with open(source_item.uri, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

            metadata = {
                "source_type": "text",
                "file_size": len(content),
                "encoding": "utf-8",
                "source_uri": source_item.uri,
                "display_uri": getattr(source_item, "display_uri", Path(source_item.uri).name),
                "mime_type": getattr(source_item, "mime_type", "text/plain"),
            }
            logger.info(f"Parsed text file: {source_item.uri}")
            return content, metadata
        except Exception as e:
            logger.error(f"Failed to parse text file {source_item.uri}: {e}")
            raise

    def parse_documents(self, source_item) -> List[Dict[str, Any]]:
        text, metadata = self.parse(source_item)
        title = metadata.get("display_uri") or Path(source_item.uri).name
        return [{
            "text": text,
            "metadata": metadata,
            "title": title,
            "source_uri": source_item.uri,
        }]


class PdfParser(BaseParser):
    def __init__(self):
        super().__init__("pdf")

    def parse(self, source_item) -> Tuple[str, Dict[str, Any]]:
        try:
            from pypdf import PdfReader
        except Exception as exc:
            raise RuntimeError(
                "PDF parsing requires pypdf. Install with: pip install pypdf"
            ) from exc

        try:
            reader = PdfReader(source_item.uri)
            page_texts: List[str] = []
            for page in reader.pages:
                page_texts.append(page.extract_text() or "")
            content = "\n\n".join(t.strip() for t in page_texts if t and t.strip())

            metadata = {
                "source_type": "pdf",
                "file_size": Path(source_item.uri).stat().st_size,
                "page_count": len(reader.pages),
                "source_uri": source_item.uri,
                "display_uri": getattr(source_item, "display_uri", Path(source_item.uri).name),
                "mime_type": getattr(source_item, "mime_type", "application/pdf"),
            }
            logger.info("Parsed PDF file: %s (pages=%s)", source_item.uri, len(reader.pages))
            return content, metadata
        except Exception as e:
            logger.error(f"Failed to parse PDF file {source_item.uri}: {e}")
            raise

    def parse_documents(self, source_item) -> List[Dict[str, Any]]:
        text, metadata = self.parse(source_item)
        title = metadata.get("display_uri") or Path(source_item.uri).name
        return [{
            "text": text,
            "metadata": metadata,
            "title": title,
            "source_uri": source_item.uri,
        }]


class WikiExtractorJsonlParser(BaseParser):
    def __init__(self):
        super().__init__("wikiextractor-jsonl")

    def parse(self, source_item) -> Tuple[str, Dict[str, Any]]:
        docs = self.parse_documents(source_item)
        if not docs:
            return "", {"source_type": "text", "source_uri": source_item.uri}
        return docs[0]["text"], docs[0].get("metadata", {})

    def parse_documents(self, source_item) -> List[Dict[str, Any]]:
        docs: List[Dict[str, Any]] = []
        with open(source_item.uri, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        data = json.loads(line)
                        docs.append({
                            "text": data.get("text", ""),
                            "title": data.get("title", "Unknown"),
                            "metadata": {
                                "source_uri": source_item.uri,
                                "source_type": "wikiextractor-jsonl",
                                "url": data.get("url", ""),
                                "word_count": len(data.get("text", "").split()),
                            }
                        })
                    except json.JSONDecodeError:
                        continue
        return docs


class ZimParser(BaseParser):
    """
    Parser for ZIM files (Wikipedia and Wikimedia content).
    
    Uses kiwix-serve + kiwix-search from kiwix-tools.
    
    The parser uses zimdump to extract content from ZIM files by:
    1. Listing all entries in the ZIM file
    2. Extracting content from multiple entries
    3. Returning structured content with metadata
    """
    def __init__(self):
        super().__init__("zim")
        self.kiwix_available = self._check_kiwix_tools()

    def _check_kiwix_tools(self) -> bool:
        """Check if kiwix tooling is available."""
        try:
            r0 = subprocess.run(["which", "kiwix-search"], capture_output=True, text=True, timeout=10)
            r1 = subprocess.run(["which", "kiwix-serve"], capture_output=True, text=True, timeout=10)
            if r0.returncode != 0 or r1.returncode != 0:
                return False
            r2 = subprocess.run(["kiwix-search", "-V"], capture_output=True, text=True, timeout=10)
            r3 = subprocess.run(["kiwix-serve", "-V"], capture_output=True, text=True, timeout=10)
            return r2.returncode == 0 and r3.returncode == 0
        except Exception:
            return False

    def parse(self, source_item) -> Tuple[str, Dict[str, Any]]:
        """
        Parse a ZIM file and extract content.
        
        Args:
            source_item: SourceItem object containing ZIM file information
            
        Returns:
            Tuple[str, Dict[str, Any]]: (content_text, metadata_dict)
            
        Note: This method requires zimdump tool to be installed in the system.
        """
        if not self.kiwix_available:
            error_msg = ("kiwix tools not found. Install with:\n"
                        "  sudo apt install kiwix-tools")
            logger.error(error_msg)
            raise RuntimeError(error_msg)
            
        try:
            # Get the ZIM file path from source_item
            zim_file = source_item.uri
            
            file_size = Path(zim_file).stat().st_size
            content = ""
            metadata = {
                "source_type": "zim",
                "file_size": file_size,
                "zim_file": Path(zim_file).name,
                "source_uri": source_item.uri,
                "entry_count": 0,
            }

            query = "Albert Einstein"
            search = subprocess.run(["kiwix-search", zim_file, query], capture_output=True, text=True, timeout=60)
            if search.returncode != 0:
                raise RuntimeError(search.stderr or "kiwix-search failed")
            content = search.stdout
            
            logger.info(f"Parsed ZIM file: {Path(zim_file).name} with {metadata['entry_count']} entries")
            return content, metadata
            
        except Exception as e:
            logger.error(f"Failed to parse ZIM file {source_item.uri}: {e}")
            raise

    def parse_documents(self, source_item) -> List[Dict[str, Any]]:
        if not self.kiwix_available:
            raise RuntimeError("kiwix tools unavailable")
        zim_file = source_item.uri
        port = 18080
        proc = subprocess.Popen(["kiwix-serve", "--port", str(port), zim_file], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            time.sleep(2)
            archive_name = self._discover_archive_name(port)
            try:
                entries = self._enumerate_entries_from_data_js(port, archive_name)
            except Exception as e:
                logger.warning("data.js discovery failed for %s: %s. Falling back to search crawl.", zim_file, e)
                docs = self._crawl_docs_from_search(port, archive_name, zim_file)
                if docs:
                    logger.info("Parsed %s ZIM article docs from %s via search crawl", len(docs), zim_file)
                    return docs
                logger.warning("Search crawl returned 0 docs for %s; using minimal kiwix-search fallback", zim_file)
                text, metadata = self.parse(source_item)
                if not text.strip():
                    return []
                title = Path(zim_file).stem
                return [{
                    "title": title,
                    "text": text,
                    "source_uri": source_item.uri,
                    "source_type": "zim",
                    "metadata": {**metadata, "fallback": "kiwix-search-minimal"},
                }]
            discovered_entries = len(entries)
            logger.info("ZIM entries discovered: %s (archive=%s)", discovered_entries, archive_name)

            skipped_records = 0
            failed_extraction = 0
            docs = []

            for entry in entries:
                try:
                    slug = (entry.get("slug") or "").strip()
                    title = self._localized_value(entry.get("title"), fallback=slug or "Untitled")
                    description = self._localized_value(entry.get("description"), fallback="")
                    speaker = (entry.get("speaker") or "").strip()

                    text_parts = [p for p in [title, f"Speaker: {speaker}" if speaker else "", description] if p]
                    text = re.sub(r"\s+", " ", "\n\n".join(text_parts)).strip()

                    if len(text) < 80:
                        skipped_records += 1
                        continue

                    source_uri = f"zim://{archive_name}/{slug or str(entry.get('id', 'unknown'))}"
                    docs.append({
                        "title": title,
                        "text": text,
                        "source_uri": source_uri,
                        "source_type": "zim",
                        "metadata": {
                            "source_type": "zim",
                            "source_uri": source_uri,
                            "title": title,
                            "speaker": speaker,
                            "slug": slug,
                            "entry_id": entry.get("id"),
                            "languages": entry.get("languages", []),
                        },
                    })
                except Exception:
                    failed_extraction += 1

            logger.info(
                "ZIM extraction stats: discovered=%s skipped=%s extracted=%s failed=%s",
                discovered_entries,
                skipped_records,
                len(docs),
                failed_extraction,
            )
            if not docs:
                logger.warning(
                    "Parsed 0 ZIM article docs from %s. discovered=%s skipped=%s failed=%s",
                    zim_file,
                    discovered_entries,
                    skipped_records,
                    failed_extraction,
                )
            else:
                logger.info("Parsed %s ZIM article docs from %s", len(docs), zim_file)

            return docs
        finally:
            proc.terminate()

    def _discover_archive_name(self, port: int) -> str:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/catalog/v2/entries?count=1", timeout=20) as r:
            xml = r.read().decode("utf-8", errors="ignore")
        m = re.search(r'href="/content/([^"]+)"', xml)
        if not m:
            raise RuntimeError("Unable to discover ZIM archive name from OPDS feed")
        return m.group(1)

    def _enumerate_entries_from_data_js(self, port: int, archive_name: str) -> List[Dict[str, Any]]:
        data_url = f"http://127.0.0.1:{port}/content/{archive_name}/assets/data.js"
        with urllib.request.urlopen(data_url, timeout=60) as r:
            js = r.read().decode("utf-8", errors="ignore")
        if "=" not in js:
            raise RuntimeError("Unexpected data.js format in ZIM archive")
        payload = js.split("=", 1)[1].strip()
        if payload.endswith(";"):
            payload = payload[:-1]
        entries = json.loads(payload)
        if not isinstance(entries, list):
            raise RuntimeError("data.js did not contain a JSON array")
        return entries

    def _localized_value(self, localized_list: Any, fallback: str = "") -> str:
        if isinstance(localized_list, str):
            return localized_list.strip() or fallback
        if isinstance(localized_list, list):
            preferred = None
            default = None
            first = None
            for item in localized_list:
                if not isinstance(item, dict):
                    continue
                text = (item.get("text") or "").strip()
                if not text:
                    continue
                lang = (item.get("lang") or "").strip().lower()
                first = first or text
                if lang == "en":
                    preferred = text
                if lang == "default":
                    default = text
            return preferred or default or first or fallback
        return fallback

    def _crawl_docs_from_search(self, port: int, archive_name: str, zim_file: str) -> List[Dict[str, Any]]:
        max_docs = int(os.getenv("LOCALWIKI_ZIM_MAX_DOCS", "400"))
        per_term_max = int(os.getenv("LOCALWIKI_ZIM_PER_TERM_MAX", "40"))
        min_chars = int(os.getenv("LOCALWIKI_ZIM_MIN_CHARS", "250"))
        max_chars = int(os.getenv("LOCALWIKI_ZIM_MAX_CHARS", "12000"))

        seed_terms = [
            "a", "e", "i", "o", "u", "the", "history", "science", "city", "person", "country",
            "new", "world", "war", "art", "music", "film", "book", "animal", "language"
        ]

        seen_sources = set()
        docs: List[Dict[str, Any]] = []

        for term in seed_terms:
            if len(docs) >= max_docs:
                break

            search_url = (
                f"http://127.0.0.1:{port}/search?content={urllib.parse.quote(archive_name)}"
                f"&pattern={urllib.parse.quote(term)}"
            )
            try:
                with urllib.request.urlopen(search_url, timeout=60) as r:
                    page = r.read().decode("utf-8", errors="ignore")
            except Exception:
                continue

            for item in self._extract_search_results(page):
                if len(docs) >= max_docs:
                    break
                source_uri = f"zim://{archive_name}/{item['slug']}"
                if source_uri in seen_sources:
                    continue
                if sum(1 for d in docs if d.get("metadata", {}).get("seed_term") == term) >= per_term_max:
                    break

                try:
                    text = self._fetch_article_text(port, item["href"])
                except Exception:
                    continue

                if len(text) < min_chars:
                    continue

                text = text[:max_chars].strip()
                docs.append({
                    "title": item["title"],
                    "text": text,
                    "source_uri": source_uri,
                    "source_type": "zim",
                    "metadata": {
                        "source_type": "zim",
                        "source_uri": source_uri,
                        "title": item["title"],
                        "seed_term": term,
                        "href": item["href"],
                        "archive": archive_name,
                        "zim_file": Path(zim_file).name,
                    },
                })
                seen_sources.add(source_uri)

        return docs

    def _extract_search_results(self, html_text: str) -> List[Dict[str, str]]:
        results: List[Dict[str, str]] = []
        seen_hrefs = set()
        for match in re.finditer(r'<a\s+href="([^"]+)"[^>]*>(.*?)</a>', html_text, re.IGNORECASE | re.DOTALL):
            href = html.unescape((match.group(1) or "").strip())
            if not href.startswith("/content/"):
                continue
            if href in seen_hrefs:
                continue
            title_html = match.group(2) or ""
            title = re.sub(r"<[^>]+>", " ", title_html)
            title = html.unescape(re.sub(r"\s+", " ", title)).strip()
            if not title:
                continue
            slug = href.rsplit("/", 1)[-1] if "/" in href else href
            results.append({"href": href, "title": title, "slug": slug})
            seen_hrefs.add(href)
        return results

    def _fetch_article_text(self, port: int, href: str) -> str:
        article_url = f"http://127.0.0.1:{port}{href}"
        with urllib.request.urlopen(article_url, timeout=60) as r:
            raw_html = r.read().decode("utf-8", errors="ignore")

        content = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", raw_html)
        content = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", content)
        content = re.sub(r"(?is)<noscript[^>]*>.*?</noscript>", " ", content)
        content = re.sub(r"(?is)<[^>]+>", " ", content)
        content = html.unescape(content)
        content = re.sub(r"\s+", " ", content).strip()
        return content


# Register parsers for different source types
class ParserFactory:
    _parsers = {
        "text": TextParser,
        "text/plain": TextParser,
        "text/markdown": TextParser,
        "text/html": TextParser,
        "application/json": TextParser,
        "pdf": PdfParser,
        "application/pdf": PdfParser,
        "wikidump": WikiExtractorJsonlParser,
        "wikiextractor-jsonl": WikiExtractorJsonlParser,
        "application/x-ndjson": WikiExtractorJsonlParser,
        "application/jsonl": WikiExtractorJsonlParser,
        "zim": ZimParser,  # Register our new ZIM parser
        "application/x-zim": ZimParser,  # Register by MIME type
    }

    @classmethod
    def register_parser(cls, source_type: str, parser_class):
        cls._parsers[source_type] = parser_class
        logger.info(f"Registered parser for {source_type}")

    @classmethod
    def create_parser(cls, source_type: str) -> BaseParser:
        if source_type in cls._parsers:
            return cls._parsers[source_type]()
        logger.warning(f"No parser registered for {source_type}, using text parser")
        return TextParser()

    @classmethod
    def create_parser_for_item(cls, source_item) -> BaseParser:
        # If source_item has a specific mime_type, use that
        if hasattr(source_item, 'mime_type') and source_item.mime_type:
            parser = cls.create_parser(source_item.mime_type)
            if parser:
                return parser
        
        # Otherwise try to determine from file extension
        uri = source_item.uri
        if uri.endswith('.zim'):
            return cls.create_parser('zim')
        if uri.endswith('.jsonl') or uri.endswith('.ndjson'):
            return cls.create_parser('wikiextractor-jsonl')
        if uri.endswith('.pdf'):
            return cls.create_parser('pdf')
        
        # Default to text parser
        return cls.create_parser('text')
