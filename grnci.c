#include "grnci.h"

#include <groonga.h>

static uint32_t
grnci_get_thread_limit(void *data)
{
  return 1;
}

static void
grnci_set_thread_limit(uint32_t new_limit, void *data)
{
}

void grnci_init_thread_limit(void)
{
  grn_thread_set_get_limit_func(grnci_get_thread_limit, NULL);
  grn_thread_set_set_limit_func(grnci_set_thread_limit, NULL);
}
