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
#include <math.h>

static int protocol_error(IOSTREAM *in, const char *id);
static int unexpected_eof(IOSTREAM *in);
static int newline_expected(IOSTREAM *in);

static atom_t ATOM_rnil;
static functor_t FUNCTOR_status1;
static functor_t FUNCTOR_prolog1;
static functor_t FUNCTOR_pair2;
static functor_t FUNCTOR_attrib2;


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
add_byte_charbuf(charbuf *cb, int c)
{ if ( ensure_space_charbuf(cb, 1) )
  { *cb->here++ = c;

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
      return unexpected_eof(in),NULL;
    if ( c == '\r' )
    { add_byte_charbuf(cb, 0);
      if ( Sgetcode(in) != '\n' )
	return newline_expected(in),NULL;
      return cb->base;
    }
    if ( c == '\n' )
    { add_byte_charbuf(cb, 0);
      return cb->base;
    }

    add_byte_charbuf(cb, c);
  }
}


		 /*******************************
		 *	      ERRORS		*
		 *******************************/

static int
protocol_error(IOSTREAM *in, const char *msg)
{ return PL_syntax_error(msg, in);
}

static int
unexpected_eof(IOSTREAM *in)
{ return protocol_error(in, "unexpected_eof");
}

static int
newline_expected(IOSTREAM *in)
{ return protocol_error(in, "newline_expected");
}



static int
redis_error(char *s, term_t msg)
{ term_t code;
  char *q;

  for(q=s; *q >= 'A' && *q <= 'Z'; q++)
    *q = *q + 'a' - 'A';

  return ( (code = PL_new_term_ref()) &&
	   PL_unify_chars(code, PL_ATOM, q-s, s) &&
	   PL_unify_term(msg,
			 PL_FUNCTOR_CHARS, "error", 2,
			   PL_FUNCTOR_CHARS, "redis_error", 2,
			     PL_TERM, code,
			     PL_STRING, q+1,
			   PL_VARIABLE) );
}

		 /*******************************
		 *	   READ MESSAGE		*
		 *******************************/

static int redis_read_stream(IOSTREAM *in, term_t message, term_t push);

#define LEN_STREAM (-2)
#define MSG_END    (-2)

static int
read_number(IOSTREAM *in, charbuf *cb, long long *vp)
{ long long v;
  char *s, *end;

  if ( !(s=read_line(in, cb)) )
    return FALSE;
  v = strtoll(s, &end, 10);
  if ( *end )
    return newline_expected(in);
  *vp = v;

  return TRUE;
}

static int
read_length(IOSTREAM *in, charbuf *cb, long long *vp)
{ char *s;

  if ( !(s=read_line(in, cb)) )
    return FALSE;
  if ( cb->base[0] == '?' )
  { *vp = LEN_STREAM;
  } else
  { long long v;
    char *end;

    v = strtoll(s, &end, 10);
    if ( *end )
      return newline_expected(in);
    *vp = v;
  }

  return TRUE;
}

static int
read_double(IOSTREAM *in, charbuf *cb, double *vp)
{ double v;
  char *s, *end;

  if ( !(s=read_line(in, cb)) )
    return FALSE;

  if ( cb->here-cb->base == 3 &&
       strncmp(cb->here, "inf", 3) == 0 )
  { v = INFINITY;
  } else if ( cb->here-cb->base == 4 &&
	      strncmp(cb->here, "-inf", 4) == 0 )
  { v = -INFINITY;
  } else
  { v = strtod(s, &end);
    if ( *end )
      return newline_expected(in),FALSE;
  }

  *vp = v;

  return TRUE;
}


static int
expect_crlf(IOSTREAM *in)
{ int c;

  if ( (c=Sgetcode(in)) == '\r' )
  { if ( Sgetcode(in) != '\n' )
      return newline_expected(in);
  } else if ( c != '\n' )
  { return newline_expected(in);
  }

  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Returns: FALSE --> error, TRUE: bulk in cb, -1: got nil.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
read_chunk(IOSTREAM *in, charbuf *cb, long long len)
{ long long i;

  for(i=0; i<len; i++)
  { int c;

    if ( (c=Sgetc(in)) == -1 )
      return unexpected_eof(in);

    if ( !add_byte_charbuf(cb, c) )
      return FALSE;
  }
  if ( !expect_crlf(in) )
    return FALSE;

  return TRUE;
}


static int
read_bulk(IOSTREAM *in, charbuf *cb)
{ long long v;

  if ( !(read_length(in, cb, &v)) )
    return FALSE;

  if ( v == LEN_STREAM )		/* RESP3 Streamed string */
  { charbuf nbuf;

    init_charbuf(&nbuf);
    empty_charbuf(cb);

    for(;;)
    { long long chlen;

      if ( Sgetc(in) != ';' )
	return protocol_error(in, "; expected");
      empty_charbuf(&nbuf);
      if ( !read_number(in, &nbuf, &chlen) )
	return FALSE;
      if ( chlen == 0 )
      { return TRUE;
      } else
      { if ( !read_chunk(in, cb, chlen) )
	  return FALSE;
      }
    }
  } else
  { if ( v == -1 )
      return -1;			/* RESP2 nil */

    empty_charbuf(cb);
    return read_chunk(in, cb, v);
  }
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Read a map to a pair  list.  Possibly   we  should  force all keys to be
acceptable for a dict and return a dict?
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
read_map(IOSTREAM *in, charbuf *cb, term_t map)
{ long long v;

  if ( !read_length(in, cb, &v) )
    return FALSE;

  if ( v == LEN_STREAM )
  { term_t head = PL_new_term_ref();
    term_t tail = PL_copy_term_ref(map);
    term_t pav  = PL_new_term_refs(2);

    for(;;)
    { int rc;

      if ( !PL_put_variable(pav+0) ||
	   !(rc=redis_read_stream(in, pav+0, 0)) )
	return FALSE;
      if ( rc == MSG_END )
	break;
      if ( !PL_unify_list(tail, head, tail) ||
	   !PL_put_variable(pav+1) ||
	   !redis_read_stream(in, pav+1, 0) ||
	   !PL_unify_term(head, PL_FUNCTOR, FUNCTOR_pair2,
			          PL_TERM, pav+0, PL_TERM, pav+1) )
	return FALSE;
    }

    return PL_unify_nil(tail);
  } else
  { term_t head = PL_new_term_ref();
    term_t tail = PL_copy_term_ref(map);
    term_t pav  = PL_new_term_refs(2);
    long long i;

    if ( v == -1 )			/* Can this happen? */
      return PL_unify_atom(map, ATOM_rnil);

    for(i=0; i<v; i++)
    { if ( !PL_unify_list(tail, head, tail) ||
	   !PL_put_variable(pav+0) ||
	   !PL_put_variable(pav+1) ||
	   !redis_read_stream(in, pav+0, 0) ||
	   !redis_read_stream(in, pav+1, 0) ||
	   !PL_unify_term(head, PL_FUNCTOR, FUNCTOR_pair2,
			          PL_TERM, pav+0, PL_TERM, pav+1) )
	return FALSE;
    }

    return PL_unify_nil(tail);
  }
}


static int
read_array(IOSTREAM *in, charbuf *cb, term_t array)
{ long long v;

  if ( !read_length(in, cb, &v) )
    return FALSE;

  if ( v == LEN_STREAM )
  { term_t head = PL_new_term_ref();
    term_t tail = PL_copy_term_ref(array);
    term_t tmp  = PL_new_term_ref();

    for(;;)
    { int rc;

      if ( !(rc=redis_read_stream(in, tmp, 0)) )
	return FALSE;
      if ( rc == MSG_END )
	break;
      if ( !PL_unify_list(tail, head, tail) ||
	   !PL_unify(head, tmp) )
	return FALSE;
    }

    return PL_unify_nil(tail);
  } else
  { term_t head = PL_new_term_ref();
    term_t tail = PL_copy_term_ref(array);
    long long i;

    if ( v == -1 )
      return PL_unify_atom(array, ATOM_rnil);

    for(i=0; i<v; i++)
    { if ( !PL_unify_list(tail, head, tail) ||
	   !redis_read_stream(in, head, 0) )
	return FALSE;
    }

    return PL_unify_nil(tail);
  }
}


static int
redis_read_stream(IOSTREAM *in, term_t message, term_t push)
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
	rc = redis_error(s, message);
      break;
    case '!':				/* RESP3 Blob error */
      if ( (rc=read_bulk(in, &cb)) )
      { assert(rc != -1);
	rc = redis_error(cb.base, message);
      }
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
    case ',':				/* RESP3 double response */
    { double v;

      rc = ( read_double(in, &cb, &v) &&
	     PL_unify_float(message, v) );

      break;
    }
    case '(':				/* RESP3 Big number */
    { if ( !(s=read_line(in, &cb)) )
      { rc = FALSE;
      } else
      { term_t t;

	rc = ( (t=PL_new_term_ref()) &&
	       PL_put_term_from_chars(t, REP_ISO_LATIN_1,
				      cb.here-cb.base, cb.base) &&
	       PL_unify(message, t) &&
	       (PL_reset_term_refs(t),TRUE) );
      }
      break;
    }
    case '#':				/* RESP3 boolean */
    { int c = Sgetcode(in);

      if ( (rc=expect_crlf(in)) )
      { if ( c == 't' || c == 'f' )
	  rc = PL_unify_bool(message, (c == 't'));
	else
	  rc = protocol_error(in, "boolean_expected");
      }
      break;
    }
    case '$':
    { if ( (rc=read_bulk(in, &cb)) )
      { if ( rc == -1 )
	{ rc = PL_unify_atom(message, ATOM_rnil);
	} else
	{ if ( cb.here-cb.base > 3 &&
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
      }

    done_bulk:
      break;
    }
    case '=':				/* RESP3 Verbatim string */
    { if ( (rc=read_bulk(in, &cb)) )
      { rc = PL_unify_chars(message, PL_STRING|REP_UTF8,
			    cb.here-cb.base-4, cb.base+4);
      }

      break;
    }
    case '~':				/* RESP3 set */
    case '*':				/* Array */
    { rc = read_array(in, &cb, message);
      break;
    }
    case '>':				/* RESP3 push */
    { term_t t;

      rc = ( push != 0 &&		/* only on toplevel term */
	     (t=PL_new_term_ref()) &&
	     PL_unify_list(push, t, push) &&
	     read_array(in, &cb, t) &&
	     (PL_reset_term_refs(t),TRUE) );
      break;
    }
    case '|':				/* RESP3 attrib */
    { term_t attrib = PL_new_term_ref();
      term_t msg    = PL_new_term_ref();

      rc = ( read_map(in, &cb, attrib) &&
	     read_map(in, &cb, msg) &&
	     PL_unify_term(message, PL_FUNCTOR, FUNCTOR_attrib2,
				      PL_TERM, attrib, PL_TERM, msg) );
      break;
    }
    case '%':				/* RESP3 map */
    { rc = read_map(in, &cb, message);
      break;
    }
    case '_':				/* RESP3 nil */
      rc = ( expect_crlf(in) &&
	     PL_unify_atom(message, ATOM_rnil) );
      break;
    case '.':
      if ( push == 0 && expect_crlf(in) )
	rc = MSG_END;
      else
	rc = protocol_error(in, "unexpected_code");
      break;
    case -1:
      rc = unexpected_eof(in);
      break;
    default:
      rc = protocol_error(in, "unexpected_code");
      break;
  }

  free_charbuf(&cb);

  return rc;
}

static foreign_t
redis_read_msg(term_t from, term_t message, term_t push)
{ IOSTREAM *in;

  if ( PL_get_stream(from, &in, SIO_INPUT) )
  { term_t tail = PL_copy_term_ref(push);
    int rc;

    rc = ( redis_read_stream(in, message, tail) &&
	   PL_unify_nil(tail) );

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
write_msg(term_t into, term_t message, int flush)
{ IOSTREAM *out;

  if ( PL_get_stream(into, &out, SIO_OUTPUT) )
  { int rc = redis_write_stream(out, message);

    if ( rc && flush )
      rc = Sflush(out) == 0;

    if ( Sferror(out) )
      rc = PL_release_stream(out);
    else
      PL_release_stream_noerror(out);

    return rc;
  }

  return FALSE;
}


static foreign_t
redis_write_msg(term_t into, term_t message)
{ return write_msg(into, message, TRUE);
}

static foreign_t
redis_write_msg_no_flush(term_t into, term_t message)
{ return write_msg(into, message, FALSE);
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
{ ATOM_rnil       = PL_new_atom("nil");
  FUNCTOR_pair2   = PL_new_functor(PL_new_atom("-"), 2);
  FUNCTOR_attrib2 = PL_new_functor(PL_new_atom("$REDISATTRIB$"), 2);
  MKFUNCTOR(status, 1);
  MKFUNCTOR(prolog, 1);

  PL_register_foreign("redis_read_msg",		  3, redis_read_msg,	       0);
  PL_register_foreign("redis_write_msg",	  2, redis_write_msg,	       0);
  PL_register_foreign("redis_write_msg_no_flush", 2, redis_write_msg_no_flush, 0);
}
