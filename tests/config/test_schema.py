from datetime import time
from types import MappingProxyType

import pytest
from pydantic import ValidationError

from tennis_booking.config.schema import (
    AppConfig,
    BookingRule,
    Profile,
    ResolvedBooking,
    Weekday,
    mask_email,
    mask_phone,
)


def make_profile(
    name: str = "roman",
    full_name: str = "Иванов Иван Иванович",
    phone: str = "+77001234567",
    email: str | None = "test@example.com",
) -> Profile:
    return Profile(name=name, full_name=full_name, phone=phone, email=email)


def make_booking(
    name: str = "Пятница вечер",
    weekday: Weekday = Weekday.FRIDAY,
    slot_local_time: str = "18:00",
    duration_minutes: int = 60,
    court_id: int = 5,
    profile: str = "roman",
    enabled: bool = True,
) -> BookingRule:
    return BookingRule(
        name=name,
        weekday=weekday,
        slot_local_time=slot_local_time,
        duration_minutes=duration_minutes,
        court_id=court_id,
        profile=profile,
        enabled=enabled,
    )


class TestProfileValid:
    def test_full_valid_profile(self) -> None:
        p = make_profile()
        assert p.name == "roman"
        assert p.full_name == "Иванов Иван Иванович"
        assert p.phone == "+77001234567"
        assert p.email == "test@example.com"

    def test_email_none(self) -> None:
        p = make_profile(email=None)
        assert p.email is None

    def test_email_empty_string_becomes_none(self) -> None:
        p = make_profile(email="")
        assert p.email is None

    def test_email_whitespace_becomes_none(self) -> None:
        p = make_profile(email="   ")
        assert p.email is None

    def test_full_name_stripped(self) -> None:
        p = make_profile(full_name="  Roman Goltsov  ")
        assert p.full_name == "Roman Goltsov"

    def test_phone_stripped(self) -> None:
        p = make_profile(phone="  +77001234567 ")
        assert p.phone == "+77001234567"

    @pytest.mark.parametrize("name", ["roman", "user-1", "user_2", "abc123", "a"])
    def test_valid_names(self, name: str) -> None:
        p = make_profile(name=name)
        assert p.name == name

    def test_frozen(self) -> None:
        p = make_profile()
        with pytest.raises(ValidationError):
            p.name = "other"  # type: ignore[misc]


class TestProfileInvalid:
    @pytest.mark.parametrize("name", ["Roman", "ROMAN", "user 1", "user.1", "", "роман", "user@x"])
    def test_invalid_names(self, name: str) -> None:
        with pytest.raises(ValidationError):
            make_profile(name=name)

    def test_full_name_empty(self) -> None:
        with pytest.raises(ValidationError):
            make_profile(full_name="")

    def test_full_name_only_whitespace(self) -> None:
        with pytest.raises(ValidationError):
            make_profile(full_name="   ")

    def test_phone_empty(self) -> None:
        with pytest.raises(ValidationError):
            make_profile(phone="")

    def test_phone_only_whitespace(self) -> None:
        with pytest.raises(ValidationError):
            make_profile(phone="   ")

    def test_extra_field_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            Profile(  # type: ignore[call-arg]
                name="roman",
                full_name="X",
                phone="+1",
                extra="boo",
            )

    def test_strict_mode_rejects_int_for_phone(self) -> None:
        with pytest.raises(ValidationError):
            Profile(name="roman", full_name="X", phone=12345)  # type: ignore[arg-type]


class TestProfileMasking:
    def test_repr_does_not_leak_full_name(self) -> None:
        p = make_profile(full_name="Очень Секретное Имя")
        assert "Очень" not in repr(p)
        assert "Секретное" not in repr(p)

    def test_repr_does_not_leak_phone(self) -> None:
        p = make_profile(phone="+77778889999")
        assert "+77778889999" not in repr(p)
        assert "8889" not in repr(p)

    def test_repr_does_not_leak_email(self) -> None:
        p = make_profile(email="topsecret@example.com")
        assert "topsecret" not in repr(p)

    def test_repr_format(self) -> None:
        p = make_profile(name="roman")
        assert repr(p) == "<profile:roman>"

    def test_str_format(self) -> None:
        p = make_profile(name="roman")
        assert str(p) == "<profile:roman>"

    def test_model_dump_unmasked(self) -> None:
        p = make_profile()
        d = p.model_dump()
        assert d["full_name"] == "Иванов Иван Иванович"
        assert d["phone"] == "+77001234567"
        assert d["email"] == "test@example.com"


class TestBookingRuleValid:
    def test_full_valid_booking(self) -> None:
        b = make_booking()
        assert b.name == "Пятница вечер"
        assert b.weekday == Weekday.FRIDAY
        assert b.slot_local_time == time(18, 0)
        assert b.duration_minutes == 60
        assert b.court_id == 5
        assert b.profile == "roman"
        assert b.enabled is True

    def test_default_enabled_true(self) -> None:
        b = BookingRule(
            name="x",
            weekday=Weekday.MONDAY,
            slot_local_time="07:00",
            duration_minutes=60,
            court_id=1,
            profile="roman",
        )
        assert b.enabled is True

    @pytest.mark.parametrize(
        ("s", "expected"),
        [
            ("00:00", time(0, 0)),
            ("07:00", time(7, 0)),
            ("18:00", time(18, 0)),
            ("23:59", time(23, 59)),
            ("12:30", time(12, 30)),
        ],
    )
    def test_valid_slot_times(self, s: str, expected: time) -> None:
        b = make_booking(slot_local_time=s)
        assert b.slot_local_time == expected

    @pytest.mark.parametrize("d", [1, 60, 90, 120, 240])
    def test_valid_durations(self, d: int) -> None:
        b = make_booking(duration_minutes=d)
        assert b.duration_minutes == d

    @pytest.mark.parametrize("c", [1, 5, 100, 9999])
    def test_valid_court_ids(self, c: int) -> None:
        b = make_booking(court_id=c)
        assert b.court_id == c

    @pytest.mark.parametrize(
        "weekday",
        [
            Weekday.MONDAY,
            Weekday.TUESDAY,
            Weekday.WEDNESDAY,
            Weekday.THURSDAY,
            Weekday.FRIDAY,
            Weekday.SATURDAY,
            Weekday.SUNDAY,
        ],
    )
    def test_all_weekdays(self, weekday: Weekday) -> None:
        b = make_booking(weekday=weekday)
        assert b.weekday == weekday

    def test_weekday_from_string(self) -> None:
        b = BookingRule(
            name="x",
            weekday="friday",  # type: ignore[arg-type]
            slot_local_time="18:00",
            duration_minutes=60,
            court_id=5,
            profile="roman",
        )
        assert b.weekday == Weekday.FRIDAY

    def test_frozen(self) -> None:
        b = make_booking()
        with pytest.raises(ValidationError):
            b.court_id = 99  # type: ignore[misc]


class TestBookingRuleInvalid:
    def test_name_empty(self) -> None:
        with pytest.raises(ValidationError):
            make_booking(name="")

    def test_name_whitespace(self) -> None:
        with pytest.raises(ValidationError):
            make_booking(name="   ")

    @pytest.mark.parametrize("d", [0, -1, 241, 1000])
    def test_invalid_duration(self, d: int) -> None:
        with pytest.raises(ValidationError):
            make_booking(duration_minutes=d)

    @pytest.mark.parametrize("c", [0, -1, -100])
    def test_invalid_court_id(self, c: int) -> None:
        with pytest.raises(ValidationError):
            make_booking(court_id=c)

    def test_invalid_weekday_string(self) -> None:
        with pytest.raises(ValidationError):
            BookingRule(
                name="x",
                weekday="funday",  # type: ignore[arg-type]
                slot_local_time="18:00",
                duration_minutes=60,
                court_id=1,
                profile="roman",
            )

    def test_weekday_capitalized_rejected(self) -> None:
        with pytest.raises(ValidationError):
            BookingRule(
                name="x",
                weekday="Friday",  # type: ignore[arg-type]
                slot_local_time="18:00",
                duration_minutes=60,
                court_id=1,
                profile="roman",
            )

    @pytest.mark.parametrize(
        "s",
        ["7:00", "07:0", "07:00:00", "24:00", "25:30", "07-00", "0700",
         "07:60", "07:99", "abc", "", "  07:00"],
    )
    def test_invalid_slot_time_strings(self, s: str) -> None:
        with pytest.raises(ValidationError):
            make_booking(slot_local_time=s)

    def test_slot_time_int_rejected(self) -> None:
        with pytest.raises(ValidationError):
            make_booking(slot_local_time=420)  # type: ignore[arg-type]

    def test_slot_time_yaml_native_int_rejected(self) -> None:
        with pytest.raises(ValidationError):
            BookingRule(
                name="x",
                weekday=Weekday.FRIDAY,
                slot_local_time=25200,  # type: ignore[arg-type]
                duration_minutes=60,
                court_id=1,
                profile="roman",
            )

    def test_slot_time_object_rejected(self) -> None:
        with pytest.raises(ValidationError):
            BookingRule(
                name="x",
                weekday=Weekday.FRIDAY,
                slot_local_time=time(7, 0),  # type: ignore[arg-type]
                duration_minutes=60,
                court_id=1,
                profile="roman",
            )

    def test_extra_field_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            BookingRule(  # type: ignore[call-arg]
                name="x",
                weekday=Weekday.FRIDAY,
                slot_local_time="18:00",
                duration_minutes=60,
                court_id=1,
                profile="roman",
                extra="hi",
            )

    @pytest.mark.parametrize("p", ["Roman", "user 1", "user.1", "", "roman!"])
    def test_invalid_profile_ref(self, p: str) -> None:
        with pytest.raises(ValidationError):
            make_booking(profile=p)


class TestResolvedBooking:
    def test_resolved_holds_profile_object(self) -> None:
        p = make_profile()
        rb = ResolvedBooking(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            duration_minutes=60,
            court_id=5,
            profile=p,
            enabled=True,
        )
        assert rb.profile is p

    def test_repr_uses_profile_repr(self) -> None:
        p = make_profile(name="roman", full_name="Очень Секретное")
        rb = ResolvedBooking(
            name="Пятница",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            duration_minutes=60,
            court_id=5,
            profile=p,
            enabled=True,
        )
        s = repr(rb)
        assert "<profile:roman>" in s
        assert "Очень" not in s
        assert "Секретное" not in s
        assert "court=5" in s
        assert "weekday=friday" in s
        assert "Пятница" in s

    def test_str_equals_repr(self) -> None:
        p = make_profile()
        rb = ResolvedBooking(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            duration_minutes=60,
            court_id=5,
            profile=p,
            enabled=True,
        )
        assert str(rb) == repr(rb)

    def test_frozen(self) -> None:
        p = make_profile()
        rb = ResolvedBooking(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            duration_minutes=60,
            court_id=5,
            profile=p,
            enabled=True,
        )
        with pytest.raises(ValidationError):
            rb.enabled = False  # type: ignore[misc]


class TestAppConfig:
    def test_construct(self) -> None:
        p = make_profile()
        rb = ResolvedBooking(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            duration_minutes=60,
            court_id=5,
            profile=p,
            enabled=True,
        )
        ac = AppConfig(
            bookings=(rb,),
            profiles=MappingProxyType({"roman": p}),
        )
        assert len(ac.bookings) == 1
        assert ac.profiles["roman"] is p

    def test_repr_does_not_leak_profile_data(self) -> None:
        p = make_profile(full_name="Очень Секретное", phone="+77778889999")
        rb = ResolvedBooking(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            duration_minutes=60,
            court_id=5,
            profile=p,
            enabled=True,
        )
        ac = AppConfig(
            bookings=(rb,),
            profiles=MappingProxyType({"roman": p}),
        )
        s = repr(ac)
        assert "Очень" not in s
        assert "Секретное" not in s
        assert "+77778889999" not in s
        assert "1 booking" in s
        assert "1 profile" in s

    def test_repr_plural(self) -> None:
        p = make_profile()
        rb1 = ResolvedBooking(
            name="a",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            duration_minutes=60,
            court_id=5,
            profile=p,
            enabled=True,
        )
        rb2 = ResolvedBooking(
            name="b",
            weekday=Weekday.SUNDAY,
            slot_local_time=time(9, 0),
            duration_minutes=60,
            court_id=6,
            profile=p,
            enabled=True,
        )
        ac = AppConfig(
            bookings=(rb1, rb2),
            profiles=MappingProxyType({"roman": p}),
        )
        s = repr(ac)
        assert "2 bookings" in s
        assert "1 profile" in s

    def test_str_equals_repr(self) -> None:
        p = make_profile()
        ac = AppConfig(
            bookings=(),
            profiles=MappingProxyType({"roman": p}),
        )
        assert str(ac) == repr(ac)

    def test_bookings_is_tuple(self) -> None:
        p = make_profile()
        ac = AppConfig(
            bookings=(),
            profiles=MappingProxyType({"roman": p}),
        )
        assert isinstance(ac.bookings, tuple)


class TestWeekdayCoercion:
    def test_weekday_int_rejected(self) -> None:
        with pytest.raises(ValidationError):
            BookingRule(
                name="x",
                weekday=5,  # type: ignore[arg-type]
                slot_local_time="18:00",
                duration_minutes=60,
                court_id=1,
                profile="roman",
            )


class TestAppConfigValidation:
    def test_profiles_must_be_mapping(self) -> None:
        with pytest.raises(ValidationError):
            AppConfig(
                bookings=(),
                profiles="not a mapping",  # type: ignore[arg-type]
            )

    def test_profiles_plain_dict_is_wrapped(self) -> None:
        p = make_profile()
        ac = AppConfig(bookings=(), profiles={"roman": p})  # type: ignore[arg-type]
        assert isinstance(ac.profiles, MappingProxyType)
        with pytest.raises(TypeError):
            ac.profiles["other"] = p  # type: ignore[index]


class TestMaskHelpers:
    def test_mask_phone_normal(self) -> None:
        assert mask_phone("+77778889999") == "+777***9999"

    def test_mask_phone_short(self) -> None:
        assert mask_phone("+1234") == "***"

    def test_mask_phone_exactly_8(self) -> None:
        assert mask_phone("12345678") == "***"

    def test_mask_phone_9_chars(self) -> None:
        assert mask_phone("123456789") == "1234***6789"

    def test_mask_email_normal(self) -> None:
        assert mask_email("roman@example.com") == "r***@example.com"

    def test_mask_email_no_at(self) -> None:
        assert mask_email("notanemail") == "***"

    def test_mask_email_empty_local(self) -> None:
        assert mask_email("@example.com") == "***@example.com"
