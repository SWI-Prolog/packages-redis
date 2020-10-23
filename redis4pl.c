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
static atom_t ATOM_atom;
static atom_t ATOM_auto;
static atom_t ATOM_string;
static atom_t ATOM_bytes;
static atom_t ATOM_codes;
static atom_t ATOM_chars;
static atom_t ATOM_integer;
static atom_t ATOM_float;
static atom_t ATOM_rational;
static atom_t ATOM_number;
static atom_t ATOM_utf8;
static atom_t ATOM_text;
static atom_t ATOM_pairs;
static atom_t ATOM_tagged_integer;
static atom_t ATOM_dict_key;
static atom_t ATOM_dict;

static functor_t FUNCTOR_status1;
static functor_t FUNCTOR_prolog1;
static functor_t FUNCTOR_pair2;
static functor_t FUNCTOR_attrib2;
static functor_t FUNCTOR_colon2;
static functor_t FUNCTOR_as2;

static int64_t MIN_TAGGED_INTEGER;
static int64_t MAX_TAGGED_INTEGER;


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
	return PL_resource_error("memory");
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


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
redis_error() returns an error in `msg`.  `msg`   is  0  if this happens
inside a nested term.  I  think  this   should  not  be  possible and be
considered a protocol error. For now we throw the error as an exception.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
redis_error(char *s, term_t msg)
{ term_t code;
  char *q;
  term_t ex;

  for(q=s; *q >= 'A' && *q <= 'Z'; q++)
    *q = *q + 'a' - 'A';

  if ( msg )
    ex = msg;
  else
    ex = PL_new_term_ref();

  if ( (code = PL_new_term_ref()) &&
       PL_unify_chars(code, PL_ATOM, q-s, s) &&
       PL_unify_term(ex,
		     PL_FUNCTOR_CHARS, "error", 2,
		       PL_FUNCTOR_CHARS, "redis_error", 2,
			 PL_TERM, code,
			 PL_STRING, q+1,
		       PL_VARIABLE) )
  { if ( msg )
      return TRUE;
    else
      return PL_raise_exception(ex);
  } else
    return FALSE;
}

		 /*******************************
		 *	   READ MESSAGE		*
		 *******************************/

typedef enum redis_type_kind
{ T_TEXT,
  T_TAGGED_INTEGER,
  T_INTEGER,
  T_FLOAT,
  T_RATIONAL,
  T_NUMBER,
  T_AUTO,
  T_PAIRS,
  T_DICT
} redis_type_kind;

typedef struct redis_type
{ redis_type_kind	kind;		/* T_ATOM, ... */
  int			pltype;		/* PL_* */
  int			encoding;	/* REP_* */
} redis_type;


static int redis_read_stream(IOSTREAM *in, term_t msgin,
			     term_t error, term_t push, redis_type *type);

static int
is_number_kind(redis_type_kind kind)
{ return kind >= T_TAGGED_INTEGER && kind <= T_NUMBER;
}


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


static int
dict_from_pairs(term_t map, term_t pairs)
{ static predicate_t pred = NULL;
  term_t av = PL_new_term_refs(3);

  if ( !pred )
    pred = PL_predicate("dict_pairs", 3, "system");

  return ( PL_put_term(av+0, map) &&
	   PL_put_term(av+2, pairs) &&
	   PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, pred, av) );
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Read a map to a pair list.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
map_length_error(term_t error, int64_t len)
{ return PL_unify_term(error,
		       PL_FUNCTOR_CHARS, "error", 2,
		         PL_FUNCTOR_CHARS, "domain_error", 2,
		           PL_CHARS, "redis_map_length",
		           PL_INT64, len,
		         PL_VARIABLE);
}


static int
read_map(IOSTREAM *in, charbuf *cb, term_t map, term_t error,
	 redis_type *type, int from_array)
{ long long v;
  redis_type *key_type, *value_type;
  term_t pairs = map;
  int rc;

  if ( type->kind == T_PAIRS || type->kind == T_DICT )
  { key_type   = &type[1];
    value_type = &type[4];
    if ( type->kind == T_DICT )
      pairs = PL_new_term_ref();
  } else
  { key_type   = type;
    value_type = type;
  }

  if ( !read_length(in, cb, &v) )
    return FALSE;

  if ( v == LEN_STREAM )
  { term_t head = PL_new_term_ref();
    term_t tail = PL_copy_term_ref(pairs);
    term_t pav  = PL_new_term_refs(2);
    int64_t len = 0;

    for(;;)
    { int rc;

      if ( !PL_put_variable(pav+0) ||
	   !(rc=redis_read_stream(in, pav+0, error, 0, key_type)) )
	return FALSE;
      if ( rc == MSG_END )
	break;
      if ( PL_unify_list(tail, head, tail) &&
	   PL_put_variable(pav+1) &&
	   (rc=redis_read_stream(in, pav+1, error, 0, value_type)) )
      { if ( rc == MSG_END )
	  return map_length_error(error, len);

	return PL_unify_term(head, PL_FUNCTOR, FUNCTOR_pair2,
			             PL_TERM, pav+0,
			             PL_TERM, pav+1);
      }

      return FALSE;
    }

    rc = PL_unify_nil(tail);
  } else
  { if ( from_array )
    { if ( v%2 )
	return map_length_error(error, v);
      v /= 2;
    }

    term_t head = PL_new_term_ref();
    term_t tail = PL_copy_term_ref(pairs);
    term_t pav  = PL_new_term_refs(2);
    long long i;

    if ( v == -1 )			/* Can this happen? */
      return PL_unify_atom(map, ATOM_rnil);

    for(i=0; i<v; i++)
    { if ( !PL_unify_list(tail, head, tail) ||
	   !PL_put_variable(pav+0) ||
	   !PL_put_variable(pav+1) ||
	   !redis_read_stream(in, pav+0, error, 0, key_type) ||
	   !redis_read_stream(in, pav+1, error, 0, value_type) ||
	   !PL_unify_term(head, PL_FUNCTOR, FUNCTOR_pair2,
			          PL_TERM, pav+0, PL_TERM, pav+1) )
	return FALSE;
    }

    rc = PL_unify_nil(tail);
  }

  if ( rc && type->kind == T_DICT )
    rc = dict_from_pairs(map, pairs);

  return rc;
}


static int
read_array(IOSTREAM *in, charbuf *cb, term_t array, term_t error,
	   redis_type *type)
{ long long v;

  if ( type->kind == T_PAIRS || type->kind == T_DICT )
    return read_map(in, cb, array, error, type, TRUE);

  if ( !read_length(in, cb, &v) )
    return FALSE;

  if ( v == LEN_STREAM )
  { term_t head = PL_new_term_ref();
    term_t tail = PL_copy_term_ref(array);
    term_t tmp  = PL_new_term_ref();

    for(;;)
    { int rc;

      if ( !(rc=redis_read_stream(in, tmp, error, 0, type)) )
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
	   !redis_read_stream(in, head, error, 0, type) )
	return FALSE;
    }

    return PL_unify_nil(tail);
  }
}


static char *
type_name(redis_type *type)
{ switch(type->kind)
  { case T_TAGGED_INTEGER:  return "tagged_integer";
    case T_INTEGER:         return "integer";
    case T_FLOAT:           return "float";
    case T_RATIONAL:        return "rational";
    case T_NUMBER:          return "number";
    default:	            return "unknown";
  }
}

enum ntype
{ N_INTEGER,
  N_RATIONAL,
  N_FLOAT
};

static int
str_is_number(size_t len, const char *data, enum ntype *nt)
{ int isnum = FALSE;
  int signok = TRUE;
  int has_dot = FALSE;
  int has_e = FALSE;
  int israt = FALSE;

  for(; len > 0; len--,data++)
  { if ( (*data == '+' || *data == '-') && signok )
    { signok = FALSE;
      continue;
    }
    if ( *data >= '0' && *data <= '9' )
    { isnum = TRUE;
      signok = FALSE;
      continue;
    }
    if ( *data == '.' && isnum && !has_dot && !israt )
    { has_dot = TRUE;
      signok = FALSE;
      continue;
    }
    if ( (*data == 'e' || *data == 'E') && isnum && !has_e && !israt )
    { has_e = TRUE;
      signok = TRUE;
      continue;
    }
    if ( *data == 'r' && isnum && !has_e && !has_dot && !israt )
    { israt = TRUE;
      continue;
    }

    return FALSE;
  }

  if ( isnum )
  { if ( israt )
      *nt = N_RATIONAL;
    else if ( has_dot || has_e )
      *nt = N_FLOAT;
    else
      *nt = N_INTEGER;
  }

  return isnum;
}

static int
compatible_num(enum ntype nt, redis_type_kind kind)
{ switch(kind)
  { case T_INTEGER:
    case T_TAGGED_INTEGER:
      return nt == N_INTEGER;
    case T_RATIONAL:
      return nt == N_INTEGER || nt == N_RATIONAL;
    case T_FLOAT:
    case T_NUMBER:
      return TRUE;
    default:
      assert(0);
      return FALSE;
  }
}


static int
is_tagged_integer(term_t t)
{ int64_t i;

  return ( PL_get_int64(t, &i) &&
	   i >= MIN_TAGGED_INTEGER && i <= MAX_TAGGED_INTEGER );
}


static int
fixup_number(term_t t, term_t message, term_t error,
	     size_t len, char *data, redis_type *type)
{ int rc;
  const char *error_name = "type_error";

  switch(type->kind)
  { case T_INTEGER:
      rc = PL_is_integer(t);
      break;
    case T_TAGGED_INTEGER:
      if ( !(rc=is_tagged_integer(t)) && PL_is_integer(t) )
	error_name = "domain_error";
      break;
    case T_FLOAT:
      if ( !(rc = PL_is_float(t)) )
      { double d;

	rc = ( PL_get_float(t, &d) &&
	       PL_put_float(t, d) );
      }
      break;
    case T_RATIONAL:
      rc = PL_is_rational(t);
      break;
    case T_NUMBER:
      rc = PL_is_number(t);
      break;
    default:
      assert(0);
      rc = FALSE;
  }

  if ( rc )
  { rc = PL_unify(message, t);
  } else
  { rc = ( PL_put_variable(t) &&
	   PL_unify_chars(t, PL_STRING|REP_UTF8, len, data) &&
	   PL_unify_term(error,
			 PL_FUNCTOR_CHARS, "error", 2,
			   PL_FUNCTOR_CHARS, error_name, 2,
			     PL_CHARS, type_name(type),
			     PL_TERM, t,
			   PL_VARIABLE) );
  }

  return rc;
}


static int
unify_bulk(term_t message, term_t error, size_t len, char *data, redis_type *type)
{ if ( len > 3 &&
       data[0] == '\0' &&
       data[2] == '\0' )
  { switch(data[1])
    { case 'T':
      { term_t t;

	return ( (t=PL_new_term_ref()) &&
		 PL_put_term_from_chars(t, REP_UTF8,
					len-3, data+3) &&
		 PL_unify(message, t) &&
		 (PL_reset_term_refs(t),TRUE) );
      }
    }
  }

  if ( type->kind == T_TEXT )
  { return PL_unify_chars(message, type->pltype|type->encoding, len, data);
  } else if ( type->kind == T_AUTO )
  { enum ntype nt;
    redis_type *ntype = &type[2];

    if ( str_is_number(len, data, &nt) &&
	 compatible_num(nt, ntype->kind) )
    { term_t t;

      if ( (t=PL_new_term_ref()) &&
	   PL_put_term_from_chars(t, REP_ISO_LATIN_1, len, data) )
      { if ( ntype->kind == T_TAGGED_INTEGER &&
	     !is_tagged_integer(t) )
	  goto as_text;

	return fixup_number(t, message, error, len, data, ntype);
      } else
	return FALSE;
    } else
    { redis_type *ttype;

    as_text:
      ttype = &type[1];
      return PL_unify_chars(message, ttype->pltype|ttype->encoding, len, data);
    }
  } else if ( type->kind >= T_TAGGED_INTEGER && type->kind <= T_NUMBER)
  { term_t t;

    return ( (t=PL_new_term_ref()) &&
	     PL_put_term_from_chars(t, REP_ISO_LATIN_1, len, data) &&
	     fixup_number(t, message, error, len, data, type) );
  } else
  { assert(0);
    return FALSE;
  }
}


static int
redis_read_stream(IOSTREAM *in, term_t message, term_t error, term_t push,
		  redis_type *type)
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
	rc = redis_error(s, error);
      break;
    case '!':				/* RESP3 Blob error */
      if ( (rc=read_bulk(in, &cb)) )
      { assert(rc != -1);
	rc = redis_error(cb.base, error);
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
	  rc = PL_unify_atom(message, ATOM_rnil);
	else
	  rc = unify_bulk(message, error, cb.here-cb.base, cb.base, type);
      }

      break;
    }
    case '=':				/* RESP3 Verbatim string */
    { if ( (rc=read_bulk(in, &cb)) )
	rc = unify_bulk(message, error, cb.here-cb.base-4, cb.base+4, type);

      break;
    }
    case '~':				/* RESP3 set */
    case '*':				/* Array */
    { rc = read_array(in, &cb, message, error, type);
      break;
    }
    case '>':				/* RESP3 push */
    { term_t t;

      rc = ( push != 0 &&		/* only on toplevel term */
	     (t=PL_new_term_ref()) &&
	     PL_unify_list(push, t, push) &&
	     read_array(in, &cb, t, error, type) &&
	     (PL_reset_term_refs(t),TRUE) );
      break;
    }
    case '|':				/* RESP3 attrib */
    { term_t attrib = PL_new_term_ref();
      term_t msg    = PL_new_term_ref();

      rc = ( read_map(in, &cb, attrib, error, type, FALSE) &&
	     redis_read_stream(in, msg, error, 0, type) &&
	     PL_unify_term(message, PL_FUNCTOR, FUNCTOR_attrib2,
				      PL_TERM, attrib, PL_TERM, msg) );
      break;
    }
    case '%':				/* RESP3 map */
    { rc = read_map(in, &cb, message, error, type, FALSE);
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

#define AS_TOP		0x0001
#define AS_PAIR_KEY	0x0002
#define AS_PAIR_VALUE	0x0004
#define AS_AUTO_TEXT	0x0008
#define AS_AUTO_NUMBER	0x0010


static int
get_as_type(term_t t, redis_type *type, int flags)
{ atom_t name;
  size_t arity;

  type->kind     = T_TEXT;
  type->pltype   = (flags&AS_PAIR_KEY) ? PL_ATOM : PL_STRING;
  type->encoding = REP_UTF8;

  if ( PL_get_name_arity(t, &name, &arity) )
  { if ( name == ATOM_atom )
      type->pltype = PL_ATOM;
    else if ( name == ATOM_string )
      type->pltype = PL_STRING;
    else if ( name == ATOM_bytes )
      type->pltype = PL_CODE_LIST, type->encoding = REP_ISO_LATIN_1;
    else if ( name == ATOM_codes )
      type->pltype = PL_CODE_LIST;
    else if ( name == ATOM_chars )
      type->pltype = PL_CHAR_LIST;
    else if ( name == ATOM_integer && arity == 0 )
      type->kind = T_INTEGER;
    else if ( name == ATOM_tagged_integer && arity == 0 )
      type->kind = T_TAGGED_INTEGER;
    else if ( name == ATOM_float && arity == 0 )
      type->kind = T_FLOAT;
    else if ( name == ATOM_rational && arity == 0 )
      type->kind = T_RATIONAL;
    else if ( name == ATOM_number && arity == 0 )
      type->kind = T_NUMBER;
    else if ( name == ATOM_auto && (arity == 0 || arity == 2) )
    { type->kind = T_AUTO;

      if ( arity == 0 )
      { type[1].kind = T_TEXT;
	type[1].pltype = PL_ATOM;
	type[1].encoding = REP_UTF8;
	type[2].kind = T_NUMBER;
      } else
      { term_t arg = PL_new_term_ref();

	return ( PL_get_arg(1, t, arg) &&
		 get_as_type(arg, &type[1], AS_AUTO_TEXT) &&
		 PL_get_arg(2, t, arg) &&
		 get_as_type(arg, &type[2], AS_AUTO_NUMBER) );
      }
    } else if ( name == ATOM_dict_key && arity == 0 )
    { type->kind = T_AUTO;
      type[1].kind = T_TEXT;
      type[1].pltype = PL_ATOM;
      type[1].encoding = REP_UTF8;
      type[2].kind = T_TAGGED_INTEGER;
    } else if ( (name == ATOM_pairs || name == ATOM_dict) &&
		arity == 2 && (flags&AS_TOP) )
    { term_t arg = PL_new_term_ref();

      type->kind = (name == ATOM_pairs ? T_PAIRS : T_DICT);
      if ( !( PL_get_arg(1, t, arg) &&
	      get_as_type(arg, &type[1], AS_PAIR_KEY) &&
	      PL_get_arg(2, t, arg) &&
	      get_as_type(arg, &type[4], AS_PAIR_VALUE) ) )
	return FALSE;
    } else if ( name == ATOM_dict && arity == 1 && (flags&AS_TOP) )
    { term_t arg = PL_new_term_ref();

      type->kind = T_DICT;
      type[1].kind = T_AUTO;
      type[2].kind = T_TEXT;
      type[2].pltype = PL_ATOM;
      type[2].encoding = REP_UTF8;
      type[3].kind = T_TAGGED_INTEGER;

      if ( !( PL_get_arg(1, t, arg) &&
	      get_as_type(arg, &type[4], AS_PAIR_VALUE) ) )
	return FALSE;
    } else
      return PL_domain_error("redis_type", t);

    if ( ((flags&AS_AUTO_NUMBER) && !is_number_kind(type->kind)) ||
	 ((flags&AS_AUTO_TEXT)   && type->kind != T_TEXT) )
      return PL_domain_error("redis_auto", t);

    if ( type->kind == T_TEXT )
    { if ( arity == 1 )
      { term_t a = PL_new_term_ref();
	atom_t an;

	_PL_get_arg(1, t, a);
	if ( !PL_get_atom_ex(a, &an) )
	  return FALSE;

	if ( an == ATOM_bytes )
	  type->encoding = REP_ISO_LATIN_1;
	else if ( an == ATOM_utf8 )
	  type->encoding = REP_UTF8;
	else if ( an == ATOM_text )
	  type->encoding = REP_MB;
	else
	  return PL_type_error("encoding", a);
      } else if ( arity != 0 )
	return PL_type_error("redis_type", t);
    }

    return TRUE;
  }

  return PL_type_error("redis_type", t);
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Type array:

  - type[0] = main type
  - if type[0] = T_PAIRS or T_DICT,
    - type [1] = key type
    - type [4] = value type
  - if type[0] = T_AUTO
    - type [1] = as text
    - type [2] = as number
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static foreign_t
redis_read_msg(term_t from, term_t msgin, term_t msgout,
	       term_t error, term_t push)
{ IOSTREAM *in;
  redis_type rt[7] = { { .kind     = T_TEXT,
                         .pltype   = PL_STRING,
                         .encoding = REP_UTF8
                       }
		     };
  term_t msg;

  if ( PL_is_functor(msgin, FUNCTOR_as2) )
  { term_t a = PL_new_term_ref();

    _PL_get_arg(2, msgin, a);
    if ( !get_as_type(a, &rt[0], AS_TOP) )
      return FALSE;
    msg = PL_new_term_ref();
    if ( !PL_unify_term(msgout, PL_FUNCTOR, FUNCTOR_as2,
			          PL_TERM, msg,
			          PL_TERM, a) )
      return FALSE;
  } else
  { msg = msgout;
  }

  if ( PL_get_stream(from, &in, SIO_INPUT) )
  { term_t tail = PL_copy_term_ref(push);
    int rc;

    rc = ( redis_read_stream(in, msg, error, tail, &rt[0]) &&
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
redis_write_key(IOSTREAM *out, term_t key)
{ term_t a1 = PL_new_term_ref();
  term_t t  = PL_copy_term_ref(key);
  charbuf cb;
  char *s;
  int rc = TRUE;
  size_t len;
  int flags = CVT_ATOM|CVT_STRING|CVT_LIST|CVT_INTEGER;

  init_charbuf(&cb);

  do
  { _PL_get_arg(1, t, a1);
    _PL_get_arg(2, t, t);

    PL_STRINGS_MARK();
    if ( (rc=PL_get_nchars(a1, &len, &s, flags)) )
    { if ( (rc=ensure_space_charbuf(&cb, len+1)) )
      { memcpy(cb.here, s, len);
	cb.here += len;
	*cb.here++ = ':';
      } else
      { rc = -1;
      }
    }
    PL_STRINGS_RELEASE();
    if ( rc != TRUE )
      goto out;
  } while( PL_is_functor(t, FUNCTOR_colon2) );

  PL_STRINGS_MARK();
  if ( (rc=PL_get_nchars(t, &len, &s, flags)) )
  { if ( (rc=ensure_space_charbuf(&cb, len)) )
    { memcpy(cb.here, s, len);
      cb.here += len;
    } else
    { rc = -1;
    }
  }
  PL_STRINGS_RELEASE();

  if ( rc == TRUE )
  { len = cb.here - cb.base;
    if ( !(Sfprintf(out, "$%zd\r\n", len) >= 0 &&
	   Sfwrite(cb.base, 1, len, out) == len &&
	   Sfprintf(out, "\r\n") >= 0) )
      rc = -1;
  }

out:
  if ( rc == FALSE )
  { rc = redis_write_one(out, key, CVT_WRITE);
  } else if ( rc == -1 )
  { rc = FALSE;
  }

  free_charbuf(&cb);
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
	} else if ( PL_is_functor(arg, FUNCTOR_colon2) )
	{ if ( !redis_write_key(out, arg) )
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
  FUNCTOR_colon2  = PL_new_functor(PL_new_atom(":"), 2);
  FUNCTOR_attrib2 = PL_new_functor(PL_new_atom("$REDISATTRIB$"), 2);

  MKATOM(atom);
  MKATOM(string);
  MKATOM(bytes);
  MKATOM(codes);
  MKATOM(chars);
  MKATOM(integer);
  MKATOM(float);
  MKATOM(rational);
  MKATOM(number);
  MKATOM(utf8);
  MKATOM(text);
  MKATOM(pairs);
  MKATOM(auto);
  MKATOM(tagged_integer);
  MKATOM(dict_key);
  MKATOM(dict);

  MKFUNCTOR(as, 2);
  MKFUNCTOR(status, 1);
  MKFUNCTOR(prolog, 1);

  PL_current_prolog_flag(PL_new_atom("min_tagged_integer"), PL_INTEGER,
			 (void*)&MIN_TAGGED_INTEGER);
  PL_current_prolog_flag(PL_new_atom("max_tagged_integer"), PL_INTEGER,
			 (void*)&MAX_TAGGED_INTEGER);

  PL_register_foreign("redis_read_msg",		  5, redis_read_msg,	       0);
  PL_register_foreign("redis_write_msg",	  2, redis_write_msg,	       0);
  PL_register_foreign("redis_write_msg_no_flush", 2, redis_write_msg_no_flush, 0);
}
