/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        jan@swi-prolog.org
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2020, SWI-Prolog Solutions b.v.
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in
       the documentation and/or other materials provided with the
       distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
    COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include <assert.h>
#include <string.h>

static int protocol_error(IOSTREAM *in);

static atom_t ATOM_rnil;
static functor_t FUNCTOR_status1;
static functor_t FUNCTOR_prolog1;

#define utf8_put_char(out, chr) \
	((chr) < 0x80 ? out[0]=(char)(chr), out+1 \
		      : _utf8_put_char(out, (chr)))


static char *
_utf8_put_char(char *out, int chr)
{ if ( chr < 0x80 )
  { *out++ = chr;
  } else if ( chr < 0x800 )
  { *out++ = 0xc0|((chr>>6)&0x1f);
    *out++ = 0x80|(chr&0x3f);
  } else if ( chr < 0x10000 )
  { *out++ = 0xe0|((chr>>12)&0x0f);
    *out++ = 0x80|((chr>>6)&0x3f);
    *out++ = 0x80|(chr&0x3f);
  } else if ( chr < 0x200000 )
  { *out++ = 0xf0|((chr>>18)&0x07);
    *out++ = 0x80|((chr>>12)&0x3f);
    *out++ = 0x80|((chr>>6)&0x3f);
    *out++ = 0x80|(chr&0x3f);
  } else if ( chr < 0x4000000 )
  { *out++ = 0xf8|((chr>>24)&0x03);
    *out++ = 0x80|((chr>>18)&0x3f);
    *out++ = 0x80|((chr>>12)&0x3f);
    *out++ = 0x80|((chr>>6)&0x3f);
    *out++ = 0x80|(chr&0x3f);
  } else if ( (unsigned)chr < 0x80000000 )
  { *out++ = 0xfc|((chr>>30)&0x01);
    *out++ = 0x80|((chr>>24)&0x3f);
    *out++ = 0x80|((chr>>18)&0x3f);
    *out++ = 0x80|((chr>>12)&0x3f);
    *out++ = 0x80|((chr>>6)&0x3f);
    *out++ = 0x80|(chr&0x3f);
  }

  return out;
}


static int
utf8_len(int chr)
{ if ( chr < 0x80 )
  { return 1;
  } else if ( chr < 0x800 )
  { return 2;
  } else if ( chr < 0x10000 )
  { return 3;
  } else if ( chr < 0x200000 )
  { return 4;
  } else if ( chr < 0x4000000 )
  { return 5;
  } else if ( (unsigned)chr < 0x80000000 )
  { return 6;
  }

  assert(0);
  return 1;
}

		 /*******************************
		 *	     CHAR BUF		*
		 *******************************/

typedef struct charbuf
{ char *base;
  char *here;
  char *end;
  char tmp[256];
} charbuf;

static void
init_charbuf(charbuf *cb)
{ cb->base = cb->here = cb->tmp;
  cb->end = &cb->tmp[sizeof(cb->tmp)/sizeof(char)];
}

static void
empty_charbuf(charbuf *cb)
{ cb->here = cb->base;
}

static void
free_charbuf(charbuf *cb)
{ if ( cb->base != cb->tmp )
    PL_free(cb->base);
}

static int
ensure_space_charbuf(charbuf *cb, size_t space)
{ if ( cb->here + space < cb->end )
  { return TRUE;
  } else
  { size_t len  = cb->end  - cb->base;
    size_t sz   = cb->here - cb->base;
    size_t nlen = len*2;

    while(cb->here-cb->base+space > nlen)
      nlen *= 2;
    if ( cb->base == cb->tmp )
    { char *n = malloc(nlen);
      if ( n )
      { memcpy(n, cb->base, sz);
	cb->base = n;
      } else
	return FALSE;
    } else
    { char *n = realloc(cb->base, nlen);
      if ( !n )
	return FALSE;
      cb->base = n;
    }
    cb->here = &cb->base[sz];
    cb->end = &cb->base[nlen];

    return TRUE;
  }
}

static int
add_utf8_charbuf(charbuf *cb, int c)
{ if ( ensure_space_charbuf(cb, 6) )
  { if ( c < 0x80 )
    { *cb->here++ = c;
    } else
    { cb->here = utf8_put_char(cb->here, c);
    }

    return TRUE;
  }

  return FALSE;
}

		 /*******************************
		 *	  READ PRIMITIVES	*
		 *******************************/

static char *
read_line(IOSTREAM *in, charbuf *cb)
{ for(;;)
  { int c = Sgetcode(in);

    if ( c == -1 )
      return protocol_error(in),NULL;
    if ( c == '\r' )
    { add_utf8_charbuf(cb, 0);
      if ( Sgetcode(in) != '\n' )
	return protocol_error(in),NULL;
      return cb->base;
    }
    if ( c == '\n' )
    { add_utf8_charbuf(cb, 0);
      return cb->base;
    }

    add_utf8_charbuf(cb, c);
  }
}


		 /*******************************
		 *	      ERRORS		*
		 *******************************/

static int
protocol_error(IOSTREAM *in)
{ return PL_syntax_error("redis protocol violation", in);
}

static int
redis_error(char *s)
{ term_t t = PL_new_term_ref();

  return ( (t = PL_new_term_ref()) &&
	   PL_unify_term(t,
			 PL_FUNCTOR_CHARS, "error", 2,
			   PL_FUNCTOR_CHARS, "redis_error", 1,
			     PL_UTF8_STRING, s,
			   PL_VARIABLE) &&
	   PL_raise_exception(t) );
}

		 /*******************************
		 *	   READ MESSAGE		*
		 *******************************/

static int
read_number(IOSTREAM *in, charbuf *cb, long long *vp)
{ long long v;
  char *s, *end;

  if ( !(s=read_line(in, cb)) )
    return FALSE;
  v = strtoll(s, &end, 10);
  if ( *end )
    return protocol_error(in);
  *vp = v;

  return TRUE;
}

static int
redis_read_stream(IOSTREAM *in, term_t message)
{ int rc = TRUE;
  int c0 = Sgetcode(in);
  charbuf cb;
  init_charbuf(&cb);
  char *s;

  switch(c0)
  { case '-':
      if ( !(s=read_line(in, &cb)) )
	rc = FALSE;
      else
	rc = redis_error(s);
      break;
    case '+':
      if ( !(s=read_line(in, &cb)) )
	rc = FALSE;
      else
	rc = PL_unify_term(message,
			   PL_FUNCTOR, FUNCTOR_status1,
			     PL_UTF8_STRING, s);
      break;
    case ':':
    { long long v;

      rc = ( read_number(in, &cb, &v) &&
	     PL_unify_int64(message, v) );

      break;
    }
    case '$':
    { long long v;

      if ( (rc=read_number(in, &cb, &v)) )
      { long long i;
	int c;

	if ( v == -1 )
	{ rc = PL_unify_atom(message, ATOM_rnil);
	  goto out;
	}

	empty_charbuf(&cb);
	for(i=0; i<v;)
	{ if ( (c=Sgetcode(in)) == -1 )
	  { rc = protocol_error(in);
	    goto out;
	  }

	  if ( !(rc=add_utf8_charbuf(&cb, c)) )
	    goto out;

	  i += utf8_len(c);
	}
	if ( i != v )
	  rc = protocol_error(in);
	if ( (c=Sgetcode(in)) == '\r' )
	{ if ( Sgetcode(in) != '\n' )
	  { rc = protocol_error(in);
	    goto out;
	  }
	} else if ( c != '\n' )
	{ rc = protocol_error(in);
	  goto out;
	}

	if ( cb.here-cb.base > 3 &&
	     cb.base[0] == '\0' &&
	     cb.base[2] == '\0' )
	{ switch(cb.base[1])
	  { case 'T':
	    { term_t t;

	      rc = ( (t=PL_new_term_ref()) &&
		     PL_put_term_from_chars(t, REP_UTF8,
					    cb.here-cb.base-3, cb.base+3) &&
		     PL_unify(message, t) &&
		     (PL_reset_term_refs(t),TRUE) );
	      goto done_bulk;
	    }
	  }
	}

	rc = PL_unify_chars(message, PL_STRING|REP_UTF8,
			    cb.here-cb.base, cb.base);
      }

    done_bulk:
      break;
    }
    case '*':
    { long long v;

      if ( (rc=read_number(in, &cb, &v)) )
      { term_t head = PL_new_term_ref();
	term_t tail = PL_copy_term_ref(message);
	long long i;

	if ( v == -1 )
	{ rc = PL_unify_atom(message, ATOM_rnil);
	  goto out;
	}

	for(i=0; i<v; i++)
	{ if ( !PL_unify_list(tail, head, tail) ||
	       !redis_read_stream(in, head) )
	  { rc = FALSE;
	    goto out;
	  }
	}

	rc = PL_unify_nil(tail);
      }

      break;
    }
    default:
      rc = protocol_error(in);
      break;
  }

out:
  free_charbuf(&cb);

  return rc;
}

static foreign_t
redis_read_msg(term_t from, term_t message)
{ IOSTREAM *in;

  if ( PL_get_stream(from, &in, SIO_INPUT) )
  { IOENC enc = in->encoding;
    in->encoding = ENC_UTF8;
    int rc = redis_read_stream(in, message);
    in->encoding = enc;

    if ( rc )
      rc = PL_release_stream(in);
    else
      PL_release_stream_noerror(in);

    return rc;
  }

  return FALSE;
}


		 /*******************************
		 *	      WRITE		*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Command is cmd(arg, ...), where arg is one of prolog(Term) or must be
converted into a string.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
redis_write_one(IOSTREAM *out, term_t t, int flags)
{ char *s;
  size_t len;
  int rc;

  PL_STRINGS_MARK();
  if ( (rc=PL_get_nchars(t, &len, &s, flags|REP_UTF8|CVT_EXCEPTION)) )
  { if ( !(Sfprintf(out, "$%zd\r\n", len) >= 0 &&
	   Sfwrite(s, 1, len, out) == len &&
	   Sfprintf(out, "\r\n") >= 0) )
      rc = FALSE;
  }
  PL_STRINGS_RELEASE();

  return rc;
}


static int
redis_write_typed(IOSTREAM *out, term_t t, int type, int flags)
{ char *s;
  size_t len;
  int rc;

  PL_STRINGS_MARK();
  if ( (rc=PL_get_nchars(t, &len, &s, flags|REP_UTF8|CVT_EXCEPTION)) )
  { if ( !(Sfprintf(out, "$%zd\r\n%c%c%c", len+3, 0, type, 0) >= 0 &&
	   Sfwrite(s, 1, len, out) == len &&
	   Sfprintf(out, "\r\n") >= 0) )
      rc = FALSE;
  }
  PL_STRINGS_RELEASE();

  return rc;
}


static int
redis_write_stream(IOSTREAM *out, term_t message)
{ atom_t name;
  size_t arity;

  if ( PL_get_name_arity(message, &name, &arity) )
  { term_t arg = PL_new_term_ref();

    if ( Sfprintf(out, "*%zd\r\n", arity+1) < 0 )
      return FALSE;
    PL_put_atom(arg, name);
    if ( redis_write_one(out, arg, CVT_ATOM) )
    { size_t i;

      for(i=1; i <= arity; i++)
      { _PL_get_arg(i, message, arg);

	if ( PL_is_atomic(arg) )
	{ if ( !redis_write_one(out, arg, CVT_ATOMIC) )
	    return FALSE;
	} else if ( PL_is_functor(arg, FUNCTOR_prolog1) )
	{ _PL_get_arg(1, arg, arg);
	  if ( !redis_write_typed(out, arg, 'T', CVT_WRITE_CANONICAL) )
	    return FALSE;
	} else
	{ if ( !redis_write_one(out, arg, CVT_WRITE) )
	    return FALSE;
	}
      }

      return TRUE;
    }
  }

  return PL_type_error("redis_message", message);
}


static foreign_t
redis_write_msg(term_t into, term_t message)
{ IOSTREAM *out;

  if ( PL_get_stream(into, &out, SIO_OUTPUT) )
  { int rc = redis_write_stream(out, message);

    if ( rc )
      rc = Sflush(out) == 0;

    if ( Sferror(out) )
      rc = PL_release_stream(out);
    else
      PL_release_stream_noerror(out);

    return rc;
  }

  return FALSE;
}




		 /*******************************
		 *	     REGISTER		*
		 *******************************/

#define MKATOM(n) \
	ATOM_ ## n = PL_new_atom(#n)
#define MKFUNCTOR(n,a) \
	FUNCTOR_ ## n ## a = PL_new_functor(PL_new_atom(#n), a)

install_t
install_redis4pl(void)
{ ATOM_rnil = PL_new_atom("nil");
  MKFUNCTOR(status, 1);
  MKFUNCTOR(prolog, 1);

  PL_register_foreign("redis_read_msg",  2, redis_read_msg,  0);
  PL_register_foreign("redis_write_msg", 2, redis_write_msg, 0);
}
