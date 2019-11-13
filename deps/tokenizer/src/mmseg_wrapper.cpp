#include "mmseg_wrapper.h"

#ifdef WIN32
#include "bsd_getopt_win.h"
#else
#include "bsd_getopt.h"
#endif

#include "Segmenter.h"
#include "SegmenterManager.h"
#include "SynonymsDict.h"
#include "ThesaurusDict.h"
#include "UnigramCorpusReader.h"
#include "UnigramDict.h"
#include "csr_utils.h"

using namespace std;
using namespace css;

#define SEGMENT_OUTPUT 1
namespace mmseg {

bool MMSEGWrapper::BuildTerms(const char* buffer, size_t buffer_size,
                                std::set<std::string>& terms) {
  std::uint64_t idx =
      seq_.fetch_add(1, std::memory_order_relaxed) % this->manager_num_;
  this->hash_lock_->Lock(idx);
  SegmenterManager* manager = this->managers_[idx];
  Segmenter* seg = manager->getSegmenter(false);

  seg->setBuffer((u1*)buffer, buffer_size);
  u2 len = 0, symlen = 0;
  u2 kwlen = 0, kwsymlen = 0;
  while (1) {
    len = 0;
    char* tok = (char*)seg->peekToken(len, symlen);
    if (!tok || !*tok || !len) break;
    seg->popToken(len);
    // skip
    if (*tok == '\r' || *tok == '\n') continue;

    // get term
    std::string term(tok, len);
    terms.insert(term);
  }
  delete seg;

  this->hash_lock_->unlock(idx);
  return true;
}

}  // namespace wwsearch
