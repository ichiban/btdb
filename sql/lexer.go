package sql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Lexer struct {
	input  string
	tokens chan token

	pos   int
	width int
}

func NewLexer(input string) *Lexer {
	return &Lexer{
		input:  input,
		tokens: make(chan token),
	}
}

func (l *Lexer) Run() {
	s := l.start()
	for s != nil {
		s = s(l.next())
	}
	close(l.tokens)
}

func (l *Lexer) Next() token {
	t := <-l.tokens
	return t
}

func (l *Lexer) next() (rune, int) {
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = w
	pos := l.pos
	l.pos += l.width
	return r, pos
}

func (l *Lexer) peek() rune {
	r, _ := l.next()
	l.backup()
	return r
}

func (l *Lexer) backup() {
	l.pos -= l.width
}

func (l *Lexer) emit(t token) {
	l.tokens <- t
}

type token struct {
	start int
	end   int
	typ   tokenType
	val   interface{}
}

func (t token) String() string {
	if t.val == nil {
		return fmt.Sprintf("%s", t.typ)
	}
	return fmt.Sprintf("%s %v", t.typ, t.val)
}

type tokenType int

// https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html

const (
	eos tokenType = iota
	errToken
	identifier
	unsignedNumeric
	characterString

	// keywords
	kwAbs
	kwAll
	kwAllocate
	kwAlter
	kwAnd
	kwAny
	kwAre
	kwArray
	kwArrayAgg
	kwArrayMaxCardinality
	kwAs
	kwAsensitive
	kwAsymmetric
	kwAt
	kwAtomic
	kwAuthorization
	kwAvg
	kwBegin
	kwBeginFrame
	kwBeginPartition
	kwBetween
	kwBigint
	kwBinary
	kwBlob
	kwBoolean
	kwBoth
	kwBy
	kwCall
	kwCalled
	kwCardinality
	kwCascaded
	kwCase
	kwCast
	kwCeil
	kwCeiling
	kwChar
	kwCharLength
	kwCharacter
	kwCharacterLength
	kwCheck
	kwClob
	kwClose
	kwCoalesce
	kwCollate
	kwCollect
	kwColumn
	kwCommit
	kwCondition
	kwConnect
	kwConstraint
	kwContains
	kwConvert
	kwCorr
	kwCorresponding
	kwCount
	kwCovarPop
	kwCovarSamp
	kwCreate
	kwCross
	kwCube
	kwCumeDist
	kwCurrent
	kwCurrentCatalog
	kwCurrentDate
	kwCurrentDefaultTransformGroup
	kwCurrentPath
	kwCurrentRole
	kwCurrentRow
	kwCurrentSchema
	kwCurrentTime
	kwCurrentTimestamp
	kwCurrentTransformGroupForType
	kwCurrentUser
	kwCursor
	kwCycle
	kwDate
	kwDay
	kwDeallocate
	kwDec
	kwDecimal
	kwDeclare
	kwDefault
	kwDelete
	kwDenseRank
	kwDeref
	kwDescribe
	kwDeterministic
	kwDisconnect
	kwDistinct
	kwDouble
	kwDrop
	kwDynamic
	kwEach
	kwElement
	kwElse
	kwEnd
	kwEndFrame
	kwEndPartition
	kwEndExec
	kwEquals
	kwEscape
	kwEvery
	kwExcept
	kwExec
	kwExecute
	kwExists
	kwExp
	kwExternal
	kwExtract
	kwFalse
	kwFetch
	kwFilter
	kwFirstValue
	kwFloat
	kwFloor
	kwFor
	kwForeign
	kwFrameRow
	kwFree
	kwFrom
	kwFull
	kwFunction
	kwFusion
	kwGet
	kwGlobal
	kwGrant
	kwGroup
	kwGrouping
	kwGroups
	kwHaving
	kwHold
	kwHour
	kwIdentity
	kwIn
	kwIndicator
	kwInner
	kwInout
	kwInsensitive
	kwInsert
	kwInt
	kwInteger
	kwIntersect
	kwIntersection
	kwInterval
	kwInto
	kwIs
	kwJoin
	kwKey // non-reserved word in SQL:2011?
	kwLag
	kwLanguage
	kwLarge
	kwLastValue
	kwLateral
	kwLead
	kwLeading
	kwLeft
	kwLike
	kwLikeRegex
	kwLn
	kwLocal
	kwLocaltime
	kwLocaltimestamp
	kwLower
	kwMatch
	kwMax
	kwMember
	kwMerge
	kwMethod
	kwMin
	kwMinute
	kwMod
	kwModifies
	kwModule
	kwMonth
	kwMultiset
	kwNational
	kwNatural
	kwNchar
	kwNclob
	kwNew
	kwNo
	kwNone
	kwNormalize
	kwNot
	kwNthValue
	kwNtile
	kwNull
	kwNullif
	kwNumeric
	kwOctetLength
	kwOccurrencesRegex
	kwOf
	kwOffset
	kwOld
	kwOn
	kwOnly
	kwOpen
	kwOr
	kwOrder
	kwOut
	kwOuter
	kwOver
	kwOverlaps
	kwOverlay
	kwParameter
	kwPartition
	kwPercent
	kwPercentRank
	kwPercentileCont
	kwPercentileDisc
	kwPeriod
	kwPortion
	kwPosition
	kwPositionRegex
	kwPower
	kwPrecedes
	kwPrecision
	kwPrepare
	kwPrimary
	kwProcedure
	kwRange
	kwRank
	kwReads
	kwReal
	kwRecursive
	kwRef
	kwReferences
	kwReferencing
	kwRegrAvgx
	kwRegrAvgy
	kwRegrCount
	kwRegrIntercept
	kwRegrR2
	kwRegrSlope
	kwRegrSxx
	kwRegrSxy
	kwRegrSyy
	kwRelease
	kwResult
	kwReturn
	kwReturns
	kwRevoke
	kwRight
	kwRollback
	kwRollup
	kwRow
	kwRowNumber
	kwRows
	kwSavepoint
	kwScope
	kwScroll
	kwSearch
	kwSecond
	kwSelect
	kwSensitive
	kwSessionUser
	kwSet
	kwSimilar
	kwSmallint
	kwSome
	kwSpecific
	kwSpecifictype
	kwSql
	kwSqlexception
	kwSqlstate
	kwSqlwarning
	kwSqrt
	kwStart
	kwStatic
	kwStddevPop
	kwStddevSamp
	kwSubmultiset
	kwSubstring
	kwSubstringRegex
	kwSucceeds
	kwSum
	kwSymmetric
	kwSystem
	kwSystemTime
	kwSystemUser
	kwTable
	kwTablesample
	kwTemporary // non-reserved word in SQL:2011?
	kwText      // non-standard
	kwThen
	kwTime
	kwTimestamp
	kwTimezoneHour
	kwTimezoneMinute
	kwTo
	kwTrailing
	kwTranslate
	kwTranslateRegex
	kwTranslation
	kwTreat
	kwTrigger
	kwTruncate
	kwTrim
	kwTrimArray
	kwTrue
	kwUescape
	kwUnion
	kwUnique
	kwUnknown
	kwUnnest
	kwUpdate
	kwUpper
	kwUser
	kwUsing
	kwValue
	kwValues
	kwValueOf
	kwVarPop
	kwVarSamp
	kwVarbinary
	kwVarchar
	kwVarying
	kwVersioning
	kwWhen
	kwWhenever
	kwWhere
	kwWidthBucket
	kwWindow
	kwWith
	kwWithin
	kwWithout
	kwYear

	asterisk
	semicolon
	leftParen
	rightParen
	comma
	plus
	minus
)

func (t tokenType) String() string {
	switch t {
	case eos:
		return "<EOS>"
	case errToken:
		return "<ERROR>"
	case identifier:
		return "<IDENTIFIER>"
	case unsignedNumeric:
		return "<UNSIGNED NUMERIC>"
	case characterString:
		return "<CHARACTER STRING>"
	case kwAbs:
		return "ABS"
	case kwAll:
		return "ALL"
	case kwAllocate:
		return "ALLOCATE"
	case kwAlter:
		return "ALTER"
	case kwAnd:
		return "AND"
	case kwAny:
		return "ANY"
	case kwAre:
		return "ARE"
	case kwArray:
		return "ARRAY"
	case kwArrayAgg:
		return "ARRAY_AGG"
	case kwArrayMaxCardinality:
		return "ARRAY_MAX_CARDINALITY"
	case kwAs:
		return "AS"
	case kwAsensitive:
		return "ASENSITIVE"
	case kwAsymmetric:
		return "ASYMMETRIC"
	case kwAt:
		return "AT"
	case kwAtomic:
		return "ATOMIC"
	case kwAuthorization:
		return "AUTHORIZATION"
	case kwAvg:
		return "AVG"
	case kwBegin:
		return "BEGIN"
	case kwBeginFrame:
		return "BEGIN_FRAME"
	case kwBeginPartition:
		return "BEGIN_PARTITION"
	case kwBetween:
		return "BETWEEN"
	case kwBigint:
		return "BIGINT"
	case kwBinary:
		return "BINARY"
	case kwBlob:
		return "BLOB"
	case kwBoolean:
		return "BOOLEAN"
	case kwBoth:
		return "BOTH"
	case kwBy:
		return "BY"
	case kwCall:
		return "CALL"
	case kwCalled:
		return "CALLED"
	case kwCardinality:
		return "CARDINALITY"
	case kwCascaded:
		return "CASCADED"
	case kwCase:
		return "CASE"
	case kwCast:
		return "CAST"
	case kwCeil:
		return "CEIL"
	case kwCeiling:
		return "CEILING"
	case kwChar:
		return "CHAR"
	case kwCharLength:
		return "CHAR_LENGTH"
	case kwCharacter:
		return "CHARACTER"
	case kwCharacterLength:
		return "CHARACTER_LENGTH"
	case kwCheck:
		return "CHECK"
	case kwClob:
		return "CLOB"
	case kwClose:
		return "CLOSE"
	case kwCoalesce:
		return "COALESCE"
	case kwCollate:
		return "COLLATE"
	case kwCollect:
		return "COLLECT"
	case kwColumn:
		return "COLUMN"
	case kwCommit:
		return "COMMIT"
	case kwCondition:
		return "CONDITION"
	case kwConnect:
		return "CONNECT"
	case kwConstraint:
		return "CONSTRAINT"
	case kwContains:
		return "CONTAINS"
	case kwConvert:
		return "CONVERT"
	case kwCorr:
		return "CORR"
	case kwCorresponding:
		return "CORRESPONDING"
	case kwCount:
		return "COUNT"
	case kwCovarPop:
		return "COVAR_POP"
	case kwCovarSamp:
		return "COVAR_SAMP"
	case kwCreate:
		return "CREATE"
	case kwCross:
		return "CROSS"
	case kwCube:
		return "CUBE"
	case kwCumeDist:
		return "CUME_DIST"
	case kwCurrent:
		return "CURRENT"
	case kwCurrentCatalog:
		return "CURRENT_CATALOG"
	case kwCurrentDate:
		return "CURRENT_DATE"
	case kwCurrentDefaultTransformGroup:
		return "CURRENT_DEFAULT_TRANSFORM_GROUP"
	case kwCurrentPath:
		return "CURRENT_PATH"
	case kwCurrentRole:
		return "CURRENT_ROLE"
	case kwCurrentRow:
		return "CURRENT_ROW"
	case kwCurrentSchema:
		return "CURRENT_SCHEMA"
	case kwCurrentTime:
		return "CURRENT_TIME"
	case kwCurrentTimestamp:
		return "CURRENT_TIMESTAMP"
	case kwCurrentTransformGroupForType:
		return "CURRENT_TRANSFORM_GROUP_FOR_TYPE"
	case kwCurrentUser:
		return "CURRENT_USER"
	case kwCursor:
		return "CURSOR"
	case kwCycle:
		return "CYCLE"
	case kwDate:
		return "DATE"
	case kwDay:
		return "DAY"
	case kwDeallocate:
		return "DEALLOCATE"
	case kwDec:
		return "DEC"
	case kwDecimal:
		return "DECIMAL"
	case kwDeclare:
		return "DECLARE"
	case kwDefault:
		return "DEFAULT"
	case kwDelete:
		return "DELETE"
	case kwDenseRank:
		return "DENSE_RANK"
	case kwDeref:
		return "DEREF"
	case kwDescribe:
		return "DESCRIBE"
	case kwDeterministic:
		return "DETERMINISTIC"
	case kwDisconnect:
		return "DISCONNECT"
	case kwDistinct:
		return "DISTINCT"
	case kwDouble:
		return "DOUBLE"
	case kwDrop:
		return "DROP"
	case kwDynamic:
		return "DYNAMIC"
	case kwEach:
		return "EACH"
	case kwElement:
		return "ELEMENT"
	case kwElse:
		return "ELSE"
	case kwEnd:
		return "END"
	case kwEndFrame:
		return "END_FRAME"
	case kwEndPartition:
		return "END_PARTITION"
	case kwEndExec:
		return "END-EXEC"
	case kwEquals:
		return "EQUALS"
	case kwEscape:
		return "ESCAPE"
	case kwEvery:
		return "EVERY"
	case kwExcept:
		return "EXCEPT"
	case kwExec:
		return "EXEC"
	case kwExecute:
		return "EXECUTE"
	case kwExists:
		return "EXISTS"
	case kwExp:
		return "EXP"
	case kwExternal:
		return "EXTERNAL"
	case kwExtract:
		return "EXTRACT"
	case kwFalse:
		return "FALSE"
	case kwFetch:
		return "FETCH"
	case kwFilter:
		return "FILTER"
	case kwFirstValue:
		return "FIRST_VALUE"
	case kwFloat:
		return "FLOAT"
	case kwFloor:
		return "FLOOR"
	case kwFor:
		return "FOR"
	case kwForeign:
		return "FOREIGN"
	case kwFrameRow:
		return "FRAME_ROW"
	case kwFree:
		return "FREE"
	case kwFrom:
		return "FROM"
	case kwFull:
		return "FULL"
	case kwFunction:
		return "FUNCTION"
	case kwFusion:
		return "FUSION"
	case kwGet:
		return "GET"
	case kwGlobal:
		return "GLOBAL"
	case kwGrant:
		return "GRANT"
	case kwGroup:
		return "GROUP"
	case kwGrouping:
		return "GROUPING"
	case kwGroups:
		return "GROUPS"
	case kwHaving:
		return "HAVING"
	case kwHold:
		return "HOLD"
	case kwHour:
		return "HOUR"
	case kwIdentity:
		return "IDENTITY"
	case kwIn:
		return "IN"
	case kwIndicator:
		return "INDICATOR"
	case kwInner:
		return "INNER"
	case kwInout:
		return "INOUT"
	case kwInsensitive:
		return "INSENSITIVE"
	case kwInsert:
		return "INSERT"
	case kwInt:
		return "INT"
	case kwInteger:
		return "INTEGER"
	case kwIntersect:
		return "INTERSECT"
	case kwIntersection:
		return "INTERSECTION"
	case kwInterval:
		return "INTERVAL"
	case kwInto:
		return "INTO"
	case kwIs:
		return "IS"
	case kwJoin:
		return "JOIN"
	case kwKey:
		return "KEY"
	case kwLag:
		return "LAG"
	case kwLanguage:
		return "LANGUAGE"
	case kwLarge:
		return "LARGE"
	case kwLastValue:
		return "LAST_VALUE"
	case kwLateral:
		return "LATERAL"
	case kwLead:
		return "LEAD"
	case kwLeading:
		return "LEADING"
	case kwLeft:
		return "LEFT"
	case kwLike:
		return "LIKE"
	case kwLikeRegex:
		return "LIKE_REGEX"
	case kwLn:
		return "LN"
	case kwLocal:
		return "LOCAL"
	case kwLocaltime:
		return "LOCALTIME"
	case kwLocaltimestamp:
		return "LOCALTIMESTAMP"
	case kwLower:
		return "LOWER"
	case kwMatch:
		return "MATCH"
	case kwMax:
		return "MAX"
	case kwMember:
		return "MEMBER"
	case kwMerge:
		return "MERGE"
	case kwMethod:
		return "METHOD"
	case kwMin:
		return "MIN"
	case kwMinute:
		return "MINUTE"
	case kwMod:
		return "MOD"
	case kwModifies:
		return "MODIFIES"
	case kwModule:
		return "MODULE"
	case kwMonth:
		return "MONTH"
	case kwMultiset:
		return "MULTISET"
	case kwNational:
		return "NATIONAL"
	case kwNatural:
		return "NATURAL"
	case kwNchar:
		return "NCHAR"
	case kwNclob:
		return "NCLOB"
	case kwNew:
		return "NEW"
	case kwNo:
		return "NO"
	case kwNone:
		return "NONE"
	case kwNormalize:
		return "NORMALIZE"
	case kwNot:
		return "NOT"
	case kwNthValue:
		return "NTH_VALUE"
	case kwNtile:
		return "NTILE"
	case kwNull:
		return "NULL"
	case kwNullif:
		return "NULLIF"
	case kwNumeric:
		return "NUMERIC"
	case kwOctetLength:
		return "OCTET_LENGTH"
	case kwOccurrencesRegex:
		return "OCCURRENCES_REGEX"
	case kwOf:
		return "OF"
	case kwOffset:
		return "OFFSET"
	case kwOld:
		return "OLD"
	case kwOn:
		return "ON"
	case kwOnly:
		return "ONLY"
	case kwOpen:
		return "OPEN"
	case kwOr:
		return "OR"
	case kwOrder:
		return "ORDER"
	case kwOut:
		return "OUT"
	case kwOuter:
		return "OUTER"
	case kwOver:
		return "OVER"
	case kwOverlaps:
		return "OVERLAPS"
	case kwOverlay:
		return "OVERLAY"
	case kwParameter:
		return "PARAMETER"
	case kwPartition:
		return "PARTITION"
	case kwPercent:
		return "PERCENT"
	case kwPercentRank:
		return "PERCENT_RANK"
	case kwPercentileCont:
		return "PERCENTILE_CONT"
	case kwPercentileDisc:
		return "PERCENTILE_DISC"
	case kwPeriod:
		return "PERIOD"
	case kwPortion:
		return "PORTION"
	case kwPosition:
		return "POSITION"
	case kwPositionRegex:
		return "POSITION_REGEX"
	case kwPower:
		return "POWER"
	case kwPrecedes:
		return "PRECEDES"
	case kwPrecision:
		return "PRECISION"
	case kwPrepare:
		return "PREPARE"
	case kwPrimary:
		return "PRIMARY"
	case kwProcedure:
		return "PROCEDURE"
	case kwRange:
		return "RANGE"
	case kwRank:
		return "RANK"
	case kwReads:
		return "READS"
	case kwReal:
		return "REAL"
	case kwRecursive:
		return "RECURSIVE"
	case kwRef:
		return "REF"
	case kwReferences:
		return "REFERENCES"
	case kwReferencing:
		return "REFERENCING"
	case kwRegrAvgx:
		return "REGR_AVGX"
	case kwRegrAvgy:
		return "REGR_AVGY"
	case kwRegrCount:
		return "REGR_COUNT"
	case kwRegrIntercept:
		return "REGR_INTERCEPT"
	case kwRegrR2:
		return "REGR_R2"
	case kwRegrSlope:
		return "REGR_SLOPE"
	case kwRegrSxx:
		return "REGR_SXX"
	case kwRegrSxy:
		return "REGR_SXY"
	case kwRegrSyy:
		return "REGR_SYY"
	case kwRelease:
		return "RELEASE"
	case kwResult:
		return "RESULT"
	case kwReturn:
		return "RETURN"
	case kwReturns:
		return "RETURNS"
	case kwRevoke:
		return "REVOKE"
	case kwRight:
		return "RIGHT"
	case kwRollback:
		return "ROLLBACK"
	case kwRollup:
		return "ROLLUP"
	case kwRow:
		return "ROW"
	case kwRowNumber:
		return "ROW_NUMBER"
	case kwRows:
		return "ROWS"
	case kwSavepoint:
		return "SAVEPOINT"
	case kwScope:
		return "SCOPE"
	case kwScroll:
		return "SCROLL"
	case kwSearch:
		return "SEARCH"
	case kwSecond:
		return "SECOND"
	case kwSelect:
		return "SELECT"
	case kwSensitive:
		return "SENSITIVE"
	case kwSessionUser:
		return "SESSION_USER"
	case kwSet:
		return "SET"
	case kwSimilar:
		return "SIMILAR"
	case kwSmallint:
		return "SMALLINT"
	case kwSome:
		return "SOME"
	case kwSpecific:
		return "SPECIFIC"
	case kwSpecifictype:
		return "SPECIFICTYPE"
	case kwSql:
		return "SQL"
	case kwSqlexception:
		return "SQLEXCEPTION"
	case kwSqlstate:
		return "SQLSTATE"
	case kwSqlwarning:
		return "SQLWARNING"
	case kwSqrt:
		return "SQRT"
	case kwStart:
		return "START"
	case kwStatic:
		return "STATIC"
	case kwStddevPop:
		return "STDDEV_POP"
	case kwStddevSamp:
		return "STDDEV_SAMP"
	case kwSubmultiset:
		return "SUBMULTISET"
	case kwSubstring:
		return "SUBSTRING"
	case kwSubstringRegex:
		return "SUBSTRING_REGEX"
	case kwSucceeds:
		return "SUCCEEDS"
	case kwSum:
		return "SUM"
	case kwSymmetric:
		return "SYMMETRIC"
	case kwSystem:
		return "SYSTEM"
	case kwSystemTime:
		return "SYSTEM_TIME"
	case kwSystemUser:
		return "SYSTEM_USER"
	case kwTable:
		return "TABLE"
	case kwTablesample:
		return "TABLESAMPLE"
	case kwText:
		return "TEXT"
	case kwThen:
		return "THEN"
	case kwTime:
		return "TIME"
	case kwTimestamp:
		return "TIMESTAMP"
	case kwTimezoneHour:
		return "TIMEZONE_HOUR"
	case kwTimezoneMinute:
		return "TIMEZONE_MINUTE"
	case kwTo:
		return "TO"
	case kwTrailing:
		return "TRAILING"
	case kwTranslate:
		return "TRANSLATE"
	case kwTranslateRegex:
		return "TRANSLATE_REGEX"
	case kwTranslation:
		return "TRANSLATION"
	case kwTreat:
		return "TREAT"
	case kwTrigger:
		return "TRIGGER"
	case kwTruncate:
		return "TRUNCATE"
	case kwTrim:
		return "TRIM"
	case kwTrimArray:
		return "TRIM_ARRAY"
	case kwTrue:
		return "TRUE"
	case kwUescape:
		return "UESCAPE"
	case kwUnion:
		return "UNION"
	case kwUnique:
		return "UNIQUE"
	case kwUnknown:
		return "UNKNOWN"
	case kwUnnest:
		return "UNNEST"
	case kwUpdate:
		return "UPDATE"
	case kwUpper:
		return "UPPER"
	case kwUser:
		return "USER"
	case kwUsing:
		return "USING"
	case kwValue:
		return "VALUE"
	case kwValues:
		return "VALUES"
	case kwValueOf:
		return "VALUE_OF"
	case kwVarPop:
		return "VAR_POP"
	case kwVarSamp:
		return "VAR_SAMP"
	case kwVarbinary:
		return "VARBINARY"
	case kwVarchar:
		return "VARCHAR"
	case kwVarying:
		return "VARYING"
	case kwVersioning:
		return "VERSIONING"
	case kwWhen:
		return "WHEN"
	case kwWhenever:
		return "WHENEVER"
	case kwWhere:
		return "WHERE"
	case kwWidthBucket:
		return "WIDTH_BUCKET"
	case kwWindow:
		return "WINDOW"
	case kwWith:
		return "WITH"
	case kwWithin:
		return "WITHIN"
	case kwWithout:
		return "WITHOUT"
	case kwYear:
		return "YEAR"
	case asterisk:
		return "*"
	case semicolon:
		return ";"
	case leftParen:
		return "("
	case rightParen:
		return ")"
	case comma:
		return ","
	default:
		return "<UNKNOWN>"
	}
}

type state func(rune, int) state

func (l *Lexer) start() state {
	return func(r rune, pos int) state {
		switch {
		case unicode.IsSpace(r):
			return l.start()
		case unicode.IsLetter(r):
			l.backup()
			return l.regularIdent(pos)
		case unicode.IsDigit(r) || r == '.':
			return l.unsignedNumericLiteral(pos)
		case r == '\'':
			return l.characterStringLiteral(pos)
		case unicode.IsPunct(r):
			l.backup()
			return l.specialChar()
		default:
			return nil
		}
	}
}

func (l *Lexer) regularIdent(start int) state {
	return func(r rune, pos int) state {
		switch {
		case unicode.IsLetter(r), unicode.IsNumber(r):
			return l.regularIdent(start)
		default:
			l.backup()
			val := l.input[start:pos]
			if t, ok := keywords[strings.ToUpper(val)]; ok {
				l.emit(token{
					start: start,
					end:   pos,
					typ:   t,
				})
				return l.start()
			}
			l.emit(token{
				start: start,
				end:   pos,
				typ:   identifier,
				val:   val,
			})
			return l.start()
		}
	}
}

func (l *Lexer) unsignedNumericLiteral(start int) state {
	return func(r rune, pos int) state {
		switch {
		case unicode.IsDigit(r):
			return l.unsignedNumericLiteral(start)
		case r == '.':
			return l.unsignedFloatLiteral(start)
		default:
			n, err := strconv.ParseInt(l.input[start:pos], 10, 64)
			if err != nil {
				l.emit(token{start: start, end: pos, typ: errToken})
				return nil
			}
			l.backup()
			l.emit(token{
				start: start,
				end:   pos,
				typ:   unsignedNumeric,
				val:   n,
			})
			return l.start()
		}
	}
}

func (l *Lexer) unsignedFloatLiteral(start int) state {
	return func(r rune, pos int) state {
		switch {
		case unicode.IsDigit(r):
			return l.unsignedFloatLiteral(start)
		default:
			n, err := strconv.ParseFloat(l.input[start:pos], 64)
			if err != nil {
				l.emit(token{start: start, end: pos, typ: errToken})
				return nil
			}
			l.backup()
			l.emit(token{
				start: start,
				end:   pos,
				typ:   unsignedNumeric,
				val:   n,
			})
			return l.start()
		}
	}
}

func (l *Lexer) characterStringLiteral(start int) state {
	return func(r rune, pos int) state {
		switch r {
		case '\'':
			return l.quoteSymbol(start)
		default:
			return l.characterStringLiteral(start)
		}
	}
}

func (l *Lexer) quoteSymbol(start int) state {
	return func(r rune, pos int) state {
		switch r {
		case '\'':
			return l.characterStringLiteral(start)
		default:
			l.backup()
			l.emit(token{
				start: start,
				end:   pos,
				typ:   characterString,
				val:   strings.Replace(l.input[start+1:pos-1], `\\`, `\`, -1),
			})
			return l.start()
		}
	}
}

func (l *Lexer) specialChar() state {
	return func(r rune, pos int) state {
		switch r {
		case '*':
			l.emit(token{start: pos, end: pos + 1, typ: asterisk})
			return l.start()
		case ';':
			l.emit(token{start: pos, end: pos + 1, typ: semicolon})
			return l.start()
		case '(':
			l.emit(token{start: pos, end: pos + 1, typ: leftParen})
			return l.start()
		case ')':
			l.emit(token{start: pos, end: pos + 1, typ: rightParen})
			return l.start()
		case ',':
			l.emit(token{start: pos, end: pos + 1, typ: comma})
			return l.start()
		case '+':
			l.emit(token{start: pos, end: pos + 1, typ: plus})
			return l.start()
		case '-':
			l.emit(token{start: pos, end: pos + 1, typ: minus})
			return l.start()
		default:
			l.emit(token{start: pos, end: pos + 1, typ: errToken})
			return nil
		}
	}
}
